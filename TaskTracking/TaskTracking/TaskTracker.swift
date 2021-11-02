import Foundation

public class TaskTracker<Task, Output, Failure>: TaskTracking where Task: Identifiable, Failure: Error {
    fileprivate typealias Controller = TaskController<Task, Output, Failure>
    
    /// Ошибки бросаемые трекером
    public enum TrackerError: Error, CustomStringConvertible {
        case nothingToCompleteError
        
        public var description: String {
            switch self {
            case .nothingToCompleteError:
                return "No tasks for completing"
            }
        }
    }
    
    /// Флаги работы трекера
    private let memoizationOptions: MemoizationOptions
    /// Очередь для выполнения переданных блоков
    private let queue: DispatchQueue
    /// Блок для запуска задач
    private let worker: (Task, @escaping (Result<Output, Failure>) throws -> ()) -> () -> ()
    /// Map соответствия контроллеров задач к ним
    private var tasks: [Task.ID: Controller]?
    /// Блокировка для корректной паралелльной работы с tasks
    private var tasksMapLock: NSRecursiveLock?
    
    public required init(memoizationOptions: MemoizationOptions, queue: DispatchQueue, worker: @escaping (Task, @escaping (Result<Output, Failure>) throws -> ()) -> () -> ()) {
        self.memoizationOptions = memoizationOptions
        self.queue = queue
        self.worker = worker
        
        if self.memoizationOptions.isReduplicate {
            self.tasks = [:]
            self.tasksMapLock = NSRecursiveLock()
        }
    }
    
    public func run(_ task: Task, completion: @escaping (Result<Output, Failure>) -> ()) -> () -> Bool {
        if self.memoizationOptions.isReduplicate {
            while true {
                self.tasksMapLock?.lock()
                defer { self.tasksMapLock?.unlock() }
                if let task = self.tasks![task.id] {
                    let (addSuccessed, taskCancel) = task.add(completion)
                    if addSuccessed {
                        return taskCancel
                    }
                } else {
                    let controller = Controller(queue: self.queue, memoizationOptions: self.memoizationOptions)
                    self.tasks![task.id] = controller
                    let (_, taskCancel) = controller.add(completion) // add always successed, because contoller in dictionary and can be nil only after finalCancel that isn't in controller yet
                    self.queue.async { [weak self] in
                        guard let self = self else {
                            return
                        }
                        let workerCancel = self.worker(task) { [weak self] result in
                            do {
                                try self?.complete(task.id, result: result)
                            } catch TrackerError.nothingToCompleteError, Controller.ControllerError.resultExisting(given: _, existed: _) {
                                // skip. Cause of error previous cancel of all tasks, or out completing before
                            }
                        }
                        let finalCancel: (() -> ()) = { [weak self] in
                            guard let self = self else {
                                return
                            }
                            self.tasksMapLock?.synchronized() {
                                if let _ = self.tasks?[task.id] {
                                    self.queue.async {
                                        workerCancel()
                                    }
                                }
                                self.tasks?[task.id] = nil
                            }
                        }
                        controller.setWorkerCancel(finalCancel)
                    }
                    return taskCancel
                }
            }
        } else {
            self.queue.async { [worker, queue] in
                _ = worker(task) { result in
                    queue.async {
                        completion(result)
                    }
                }
            }
            return { false }
        }
    }
    
    public func complete(_ taskId: Task.ID, result: Result<Output, Failure>) throws {
        var task: Controller?
        self.tasksMapLock?.synchronized {
            task = self.tasks?[taskId]
            if !self.memoizationOptions.isMemorized {
                self.tasks?[taskId] = nil
            }
        }
        if let task = task {
            do {
                try task.complete(result)
            } catch Controller.ControllerError.nothingToCompleteError {
                throw TrackerError.nothingToCompleteError
            }
        } else {
            throw TrackerError.nothingToCompleteError
        }
    }
    
    public func result(for taskId: Task.ID) -> Result<Output, Failure>? {
        self.tasksMapLock?.synchronized {
            return tasks?[taskId]?.result
        }
    }
}

fileprivate class TaskController<Task, Output, Failure> where Task: Identifiable, Failure: Error {
    typealias CompletionBlock = (Result<Output, Failure>) -> ()
    typealias TaskCancelBlock = () -> Bool
    typealias FinalCancelBlock = () -> ()
    
    /// Ошибки бросаемые TaskControllerom
    public enum ControllerError: Error, CustomStringConvertible {
        case resultExisting(given: Result<Output, Failure>, existed: Result<Output, Failure>)
        case error(message: String)
        case nothingToCompleteError
        
        public var description: String {
            switch self {
            case .resultExisting(given: let given, existed: let existed):
                return "Result has already existed: \(existed). New result ignored \(given)"
            case .error(message: let message):
                return message
            case .nothingToCompleteError:
                return "All tasks have been canceled"
            }
        }
    }
    
    /// Состояние контроллера и всех задач
    private enum State {
        case completed, canceled, processing
    }
    
    /// Блокировка для корректной работы с контроллером
    private let lock: NSRecursiveLock = NSRecursiveLock()
    /// Map CompletionBlock'ов для запуска после выполнения первой запущенной задачи
    private var completions: [UUID: CompletionBlock] = [:]
    /// Блок отмены действий, когда все задачи отменены
    private var finalCancel: FinalCancelBlock?
    /// Очередь для выполнения переданных блоков
    private let queue: DispatchQueue
    /// Флаги работы трекера
    private let memoizationOptions: MemoizationOptions
    /// Состояние контроллера
    private var state: State
    /// Результат работы задачи, если требуется его запоминать
    public var result: Result<Output, Failure>?
    
    init(queue: DispatchQueue, memoizationOptions: MemoizationOptions) {
        self.queue = queue
        self.memoizationOptions = memoizationOptions
        self.state = .processing
    }
    
    /// Внешняя установка finalCancel и запуска, если уже всё было отменено
    public func setWorkerCancel(_ cancel: @escaping FinalCancelBlock) {
        self.lock.synchronized {
            self.finalCancel = cancel
            if state == .canceled {
                cancel()
            }
        }
    }
    
    /// Завершение работы с полученным результатом и передача его всем completion блокам.
    /// Сохранение результата, если memoizationOptions этого требует
    public func complete(_ result: Result<Output, Failure>) throws {
        var completionsBlocks: [CompletionBlock]?
        try self.lock.synchronized {
            if self.state == .canceled {
                throw ControllerError.nothingToCompleteError
            }
            if let existedResult = self.result {
                throw ControllerError.resultExisting(given: result, existed: existedResult)
            } else {
                self.state = .completed
                if self.memoizationOptions.isMemorized {
                    self.result = result
                } else {
                    do {
                        _ = try result.get()
                        if self.memoizationOptions.contains(.succeeded) {
                            self.result = result
                        }
                    } catch {
                        if self.memoizationOptions.contains(.failed) {
                            self.result = result
                        }
                    }
                }
            }
            completionsBlocks = Array(self.completions.values)
            self.completions = [:]
        }
        completionsBlocks?.forEach { completion in
            self.queue.async {
                completion(result)
            }
        }
    }
    
    /// Добавление задачи в ожидание на завершение.
    /// - Returns: Блок отмены выполнения переданой задачи
    public func add(_ completion: @escaping CompletionBlock) -> (Bool, TaskCancelBlock) {
        self.lock.synchronized {
            if self.state == .canceled {
                return (false, { false })
            }
            if let result = result {
                completion(result)
                return (true, { false })
            }
            let uuid = UUID()
            completions[uuid] = completion
            return (
                true,
                {
                    self.lock.synchronized {
                        if let _ = self.completions[uuid] {
                            self.completions[uuid] = nil
                            if self.completions.isEmpty && self.state == .processing {
                                self.state = .canceled
                                if let finalCancel = self.finalCancel {
                                    finalCancel()
                                }
                            }
                            return true
                        } else {
                            return false
                        }
                    }
                }
            )
        }
    }
}

fileprivate extension NSLocking {
    func synchronized<R>(_ block: () throws -> R) rethrows -> R {
        self.lock()
        defer { self.unlock() }
        return try block()
    }
}

public extension MemoizationOptions {
    /// Флаг хранения результатов
    var isMemorized: Bool {
        return self.contains(.succeeded)
            || self.contains(.failed)
            || self.contains(.completed)
    }

    /// Флаг работы с дублирующими задачами
    var isReduplicate: Bool {
            return !self.isEmpty
    }
}
