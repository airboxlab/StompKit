//
//  StompKit.m
//  StompKit
//
//  Created by Jeff Mesnil on 09/10/2013.
//  Copyright (c) 2013 Jeff Mesnil. All rights reserved.
//


import CocoaAsyncSocket

fileprivate let kVersion1_2 = "1.2"
fileprivate let kDefaultTimeout = 10
fileprivate let kNoHeartBeat = "0,0"

fileprivate let kLineFeed : String = "\n"
fileprivate let	kNullChar : String = "\0"
fileprivate let kHeaderSeparator : String = ":"

fileprivate let kHeaderAcceptVersion = "accept-version"
fileprivate let kHeaderAck           = "ack"
fileprivate let kHeaderContentLength = "content-length"
fileprivate let kHeaderDestination   = "destination"
fileprivate let kHeaderHeartBeat     = "heart-beat"
fileprivate let kHeaderHost          = "host"
fileprivate let kHeaderID            = "id"
fileprivate let kHeaderLogin         = "login"
fileprivate let kHeaderMessage       = "message"
fileprivate let kHeaderPasscode      = "passcode"
fileprivate let kHeaderReceipt       = "receipt"
fileprivate let kHeaderReceiptID     = "receipt-id"
fileprivate let kHeaderSession       = "session"
fileprivate let kHeaderSubscription  = "subscription"
fileprivate let kHeaderTransaction   = "transaction"


fileprivate let kCommandAbort       = "ABORT"
fileprivate let kCommandAck         = "ACK"
fileprivate let kCommandBegin       = "BEGIN"
fileprivate let kCommandCommit      = "COMMIT"
fileprivate let kCommandConnect     = "CONNECT"
fileprivate let kCommandConnected   = "CONNECTED"
fileprivate let kCommandDisconnect  = "DISCONNECT"
fileprivate let kCommandError       = "ERROR"
fileprivate let kCommandMessage     = "MESSAGE"
fileprivate let kCommandNack        = "NACK"
fileprivate let kCommandReceipt     = "RECEIPT"
fileprivate let kCommandSend        = "SEND"
fileprivate let kCommandSubscribe   = "SUBSCRIBE"
fileprivate let kCommandUnsubscribe = "UNSUBSCRIBE"


typealias STOMPFrameHandler = (_ frame: STOMPFrame) -> Void
typealias STOMPMessageHandler = (_ message: STOMPMessage) -> Void

class STOMPFrame {
	internal let command: String
	internal let headers : [String: Any]
	internal let body : String?
	
	init(command: String, headers: [String: Any]?, body: String?){
		self.command = command
		self.headers = headers ?? [:]
		self.body = body
	}
	
	func toData() -> Data {
		return self.toString().data(using: String.Encoding.utf8)!
	}
	
	func toString() -> String {
		let frame : NSMutableString = NSMutableString.init(string: self.command.appending(kLineFeed))
		
		for (key, value) in self.headers {
			frame.append("\(key)\(kHeaderSeparator)\(value)\(kLineFeed)")
		}
		
		frame.append(kLineFeed)
		if self.body != nil {
			frame.append(self.body!)
		}
		
		frame.append(kNullChar)
		return frame as String
	}
	
	static func fromData(data: Data) -> STOMPFrame? {
		
		let strData : Data = data.subdata(in: 0..<data.count)
		
		guard let msg : String = String(data: strData, encoding: .utf8) else {
			return nil
		}
		
		var contents : [String] = msg.components(separatedBy: kLineFeed)
		
		while contents.count > 0 && contents[0] == "" {
			contents.remove(at: 0)
		}
		
		let command : String = contents[0]
		var headers : [String: String] = [:]
		var body : String = String()
		
		var hasHeaders : Bool = false
		
		for line in contents {
			if hasHeaders {
				for c in line {
					if (String(c) != kNullChar) {
						body.append(String(c))
					}
				}
			} else {
				if line == "" {
					hasHeaders = true
				} else {
					let parts : NSMutableArray = NSMutableArray.init(array: line.components(separatedBy: kHeaderSeparator))
					
					let key : String = parts[0] as! String
					parts.removeObject(at: 0)
					headers[key] = parts.componentsJoined(by: kHeaderSeparator)
				}
				
			}
		}
		
		return STOMPFrame(command: command, headers: headers, body: body)
	}
}

class STOMPMessage : STOMPFrame {
	private let client : STOMPClient
	
	init(client: STOMPClient, headers: [String:Any], body: String?){
		self.client = client
		super.init(command: kCommandMessage, headers: headers, body: body)
	}
	
	func ack(command: String, headers: [String:Any]?) {
		var ackHeaders : [String:String] = [:]
		ackHeaders[kHeaderID] = self.headers[kHeaderAck] as? String
		self.client.sendFrame(command: command, headers: ackHeaders, body: nil)
	}
	
	func ack(headers: [String:Any]?) {
		self.ack(command: kCommandAck, headers: headers)
	}
	
	func nack(headers: [String:Any]?){
		self.ack(command: kCommandNack, headers: headers)
	}
	
	static func fromFrame(frame: STOMPFrame, client: STOMPClient) -> STOMPMessage {
		return STOMPMessage(client: client, headers: frame.headers, body: frame.body)
	}
}

class STOMPSubscription {
	private let client : STOMPClient
	private let identifier : String

	init(client: STOMPClient, identifier: String){
		self.client = client
		self.identifier = identifier
	}
	
	func unsubscribe(){
		self.client.sendFrame(command: kCommandUnsubscribe, headers: [kHeaderID: self.identifier], body: nil)
	}
	
	func description() -> String {
		return "<STOMPSubscription identifier:\(self.identifier)>"
	}
}

class STOMPTransaction {
	private let client: STOMPClient
	private let identifier : String
	
	init(client: STOMPClient, identifier: String) {
		self.client = client
		self.identifier = identifier
	}
	
	func commit(){
		self.client.sendFrame(command: kCommandCommit, headers: [kHeaderTransaction: self.identifier], body: nil)
	}
	
	func abort(){
		self.client.sendFrame(command: kCommandAbort, headers: [kHeaderTransaction: self.identifier], body: nil)
	}
}

class STOMPClient : NSObject, GCDAsyncSocketDelegate {
    
	private var login : String! = nil
	private var passcode : String! = nil
	private var ssl : Bool = false
	private var receiptHandler : STOMPFrameHandler? = nil
	private var errorHandler : ((_ error: Error?) -> Void)? = nil
	private var connected : Bool = false
	
	private var socket : GCDAsyncSocket! = nil
	private var host : String! = nil
	private var port : UInt16! = nil
	private var clientHeartBeat : String! = nil
	private weak var pinger : Timer! = nil
	private weak var ponger : Timer! = nil
	
	var disconnectHandler : ((_ error: Error?) -> Void)? = nil
	private var connectionCompletionHandler: ((_ connectedFrame: STOMPFrame?, _ error: Error?) -> Void)? = nil
	
	private var subscriptions : [String:Any]! = nil
	
	private var idGenerator : Int
	private var serverActivity : CFAbsoluteTime = 0
	
	
	init(host: String, port: UInt16){
		self.host = host
		self.port = port
		self.idGenerator = 0
		self.connected = false
		self.subscriptions = [:]
		self.clientHeartBeat = "5000,10000"
        
		super.init()
        
		self.socket = GCDAsyncSocket(delegate: self, delegateQueue: DispatchQueue.global(qos: .default))
	}
	
	func connect(login: String, passcode: String, hostHeader: String, useSSL : Bool, completionHandler : @escaping (_ connectedFrame: STOMPFrame?, _ error: Error?) -> Void){
		self.connect(headers: [kHeaderLogin: login, kHeaderPasscode: passcode, "host": hostHeader], completionHandler: completionHandler, useSSL: useSSL)
	}
	
	func connect(login: String, passcode: String, completionHandler : @escaping (_ connectedFrame: STOMPFrame?, _ error: Error?) -> Void){
		self.connect(headers: [kHeaderLogin: login, kHeaderPasscode: passcode], completionHandler: completionHandler)
	}
	
	func connect(headers: [String:Any], completionHandler : @escaping (_ connectedFrame: STOMPFrame?, _ error: Error?) -> Void, useSSL : Bool){
		self.connectionCompletionHandler = completionHandler
		
		if useSSL {
			let _ : Error?
			do {
				try self.socket.connect(toHost: self.host, onPort: self.port)
                self.socket.startTLS(["GCDAsyncSocketManuallyEvaluateTrust": true as NSObject])
			} catch {
				self.connectionCompletionHandler?(nil, error)
			}
		} else {
			let _ : Error?
			do {
				try self.socket.connect(toHost: self.host, onPort: self.port)
			} catch {
				self.connectionCompletionHandler?(nil, error)
			}
		}
		
		var connectHeaders : [String:Any] = headers
		connectHeaders[kHeaderAcceptVersion] = kVersion1_2
		if connectHeaders[kHeaderHost] == nil {
			connectHeaders[kHeaderHost] = self.host
		}
		
		if connectHeaders[kHeaderHeartBeat] == nil {
			connectHeaders[kHeaderHeartBeat] = self.clientHeartBeat
		} else {
			self.clientHeartBeat = connectHeaders[kHeaderHeartBeat] as! String
		}
		
		self.sendFrame(command: kCommandConnect, headers: connectHeaders, body: nil)
	}
	
	func connect(headers: [String:Any], completionHandler : @escaping (_ connectedFrame: STOMPFrame?, _ error: Error?) -> Void){
		self.connectionCompletionHandler = completionHandler
		
		var _ : Error? = nil
		
		do {
			try self.socket.connect(toHost: self.host, onPort: self.port)
		} catch  {
			self.connectionCompletionHandler?(nil, nil)
		}
		
		var connectHeaders : [String:Any] = headers
		connectHeaders[kHeaderAcceptVersion] = kVersion1_2
		if connectHeaders[kHeaderHost] == nil {
			connectHeaders[kHeaderHost] = self.host
		}
		
		if connectHeaders[kHeaderHeartBeat] == nil {
			connectHeaders[kHeaderHeartBeat] = self.clientHeartBeat
		} else {
			self.clientHeartBeat = connectHeaders[kHeaderHeartBeat] as! String
		}
		
		self.sendFrame(command: kCommandConnect, headers: connectHeaders, body: nil)
	}
	
	func sendTo(destination: String, body: String?) {
		self.sendTo(destination: destination, headers: nil, body: body)
	}
	
	func sendTo(destination: String, headers: [String:Any]?, body: String?){
		
		var msgHeaders : [String:Any] = headers ?? [:]
		msgHeaders[kHeaderDestination] = destination
		
		if body != nil {
			msgHeaders[kHeaderContentLength] = body?.count ?? 0
		}
		
		self.sendFrame(command: kCommandSend, headers: msgHeaders, body: body)
	}
	
	func subscribeTo(destination: String, messageHandler: STOMPMessageHandler?) -> STOMPSubscription {
		return self.subscribeTo(destination: destination, headers: nil, messageHandler: messageHandler)
	}
	
	func subscribeTo(destination: String, headers: [String:Any]?, messageHandler: STOMPMessageHandler?) -> STOMPSubscription {
		var subHeaders : [String: Any] = headers ?? [:]
		
		subHeaders[kHeaderDestination] = destination
		var identifier : String? = subHeaders[kHeaderID] as? String
		if identifier == nil {
			identifier = "sub-\(idGenerator)"
			idGenerator += 1
			
			subHeaders[kHeaderID] = identifier!
		}
		
		self.subscriptions[identifier!] = messageHandler
		self.sendFrame(command: kCommandSubscribe, headers: subHeaders, body: nil)
		
		return STOMPSubscription(client: self, identifier: identifier!)
	}
	
	func begin() -> STOMPTransaction {
		let identifier = "tx-\(idGenerator)"
		idGenerator += 1
		return self.begin(identifier: identifier)
	}
	
	func begin(identifier: String) -> STOMPTransaction {
		self.sendFrame(command: kCommandBegin, headers: [kHeaderTransaction: identifier], body: nil)
		return STOMPTransaction(client: self, identifier: identifier)
	}
	
	func disconnect(){
		self.disconnect(completion: nil)
	}
	
	func disconnect(completion handler: ((_ error : Error?) -> Void)?) {
//		self.disconnectHandler = handler
		
		self.sendFrame(command: kCommandDisconnect, headers: nil, body: nil)
		self.subscriptions.removeAll()
		self.pinger?.invalidate()
		self.ponger?.invalidate()
		self.socket.disconnectAfterReadingAndWriting()
	}
	
	fileprivate func sendFrame(command: String, headers: [String:Any]?, body: String?){
		if self.socket.isDisconnected() {
			return
		}
		
		let frame : STOMPFrame = STOMPFrame(command: command, headers: headers, body: body)
//		print("send frame \(frame.command)")
		
		let data : Data = frame.toData()
		self.socket.write(data, withTimeout: TimeInterval(kDefaultTimeout), tag: 123)
	}
	
	@objc fileprivate func sendPing(timer: Timer) {
		if self.socket.isDisconnected() {
			return
		}
		
		self.socket.write(GCDAsyncSocket.lfData(), withTimeout: TimeInterval(kDefaultTimeout), tag: 123)
//		print(">>> PING")
	}
	
	@objc fileprivate func checkPong(timer: Timer){
		let dict : [String:Any] = timer.userInfo as? [String:Any] ?? [:]
		
		let ttl : Int = (dict["ttl"] as? Int) ?? 0
		
		let delta : CFAbsoluteTime = CFAbsoluteTimeGetCurrent() - serverActivity
		if delta > Double(ttl * 2) {
			print("did not receive server activity for the last \(delta) seconds")
			self.disconnect(completion: self.errorHandler)
		}
	}
	
	fileprivate func setupHeartBeat(clientValues : String, serverValues: String){
		var cx : Int = 0, cy : Int = 0, sx : Int = 0, sy : Int = 0
		
		
		
		var scanner : Scanner = Scanner(string: clientValues)
		scanner.charactersToBeSkipped = CharacterSet(charactersIn: ", ")
		scanner.scanInt(&cx)
		scanner.scanInt(&cy)
		
		scanner = Scanner(string: serverValues)
		scanner.charactersToBeSkipped = CharacterSet(charactersIn: ", ")
		scanner.scanInt(&sx)
		scanner.scanInt(&sy)
		
		let pingTTL : Int = Int(ceil(Double(max(cx, sy) / 1000)))
		let pongTTL : Int = Int(ceil(Double(max(sx, cy) / 1000)))
		
//		print("send heart-beat every \(pingTTL) seconds")
//		print("expect to receive heart-beats every \(pongTTL) seconds")
//		
		
		DispatchQueue.main.async {
			if pingTTL > 0 {
				self.pinger = Timer.scheduledTimer(timeInterval: TimeInterval(pingTTL), target: self, selector: #selector(self.sendPing), userInfo: nil, repeats: true)
			}
			
			if pongTTL > 0 {
				self.ponger = Timer.scheduledTimer(timeInterval: TimeInterval(pongTTL), target: self, selector: #selector(self.checkPong), userInfo: ["ttl" : pongTTL], repeats: true)
			}
		}
	}
	
	func receive(frame: STOMPFrame){
		if frame.command == kCommandConnected {
			self.connected = true
			self.setupHeartBeat(clientValues: self.clientHeartBeat, serverValues: frame.headers[kHeaderHeartBeat] as! String)
			
			self.connectionCompletionHandler?(frame, nil)
		} else if frame.command == kCommandMessage {
			if let key = frame.headers[kHeaderSubscription] as? String {
				if let handler : STOMPMessageHandler = self.subscriptions[key] as? STOMPMessageHandler {
					let message : STOMPMessage = STOMPMessage.fromFrame(frame: frame, client: self)
					handler(message)
				} else {
					
				}
			}
			
		} else if frame.command == kCommandReceipt {
			self.receiptHandler?(frame)
		} else if frame.command == kCommandError {
			let error : NSError = NSError(domain: "StompKit", code: 1, userInfo: ["frame" : frame])
			
			if self.connected {
				self.connectionCompletionHandler?(frame, error)
			} else {
				self.errorHandler?(error)
//				print("ERROR \(frame)")
			}
		}
	}
	
	func readFrame(){
		self.socket.readData(to: GCDAsyncSocket.zeroData(), withTimeout: -1, tag: 0)
	}
	
    func socket(_ sock: GCDAsyncSocket, didRead data: Data, withTag tag: Int) {
		self.serverActivity = CFAbsoluteTimeGetCurrent()
		
		guard let frame : STOMPFrame = STOMPFrame.fromData(data: data) else {
			return
		}

		self.receive(frame: frame)
		self.readFrame()
	}
	
    func socket(_ sock: GCDAsyncSocket, didConnectToHost host: String, port: UInt16) {
		self.readFrame()
	}
	
    func socketDidDisconnect(_ sock: GCDAsyncSocket, withError err: Error?) {
//		print("socket did disconnect")
//		print("error : \(err?.localizedDescription)")
		
		if self.connected && (self.connectionCompletionHandler != nil) {
			self.connectionCompletionHandler?(nil, nil)
		} else if self.connected {
			self.disconnectHandler?(err)
			self.errorHandler?(err)
		}
	}
	
    func socket(_ sock: GCDAsyncSocket, didReadPartialDataOfLength partialLength: UInt, tag: Int) {
//		print("PONG \(partialLength)")
		self.serverActivity = CFAbsoluteTimeGetCurrent()
	}
}

func max(x: Int, y: Int) -> Int {
	if x < y {
		return y
	} else {
		return x
	}
}
