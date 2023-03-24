use crate::capabilities::server_capabilities;
use crate::config::{Config, CONFIG_KEY};
use crate::{convert, handler, lsp_ext, LspError, UrlExt, Vfs, MAX_FILE_LEN};
use anyhow::{anyhow, bail, Context, Result};
use ide::{Analysis, AnalysisHost, Cancelled, FlakeInfo, VfsPath};
use lsp_server::{ErrorCode, Message, Notification, ReqQueue, Request, RequestId, Response};
use lsp_types::notification::Notification as _;
use lsp_types::{
    notification as notif, request as req, ConfigurationItem, ConfigurationParams, Diagnostic,
    InitializeParams, InitializeResult, MessageType, NumberOrString, PublishDiagnosticsParams,
    ServerInfo, ShowMessageParams, Url,
};
use nix_interop::nixos_options::NixosOptions;
use nix_interop::{flake_lock, FLAKE_FILE, FLAKE_LOCK_FILE};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io::ErrorKind;
use std::panic::UnwindSafe;
use std::path::Path;
use std::sync::{Arc, Once, RwLock};
use std::{fs, panic};
use tokio::sync::mpsc;

const NIXOS_OPTIONS_FLAKE_INPUT: &str = "nixpkgs";

type ReqHandler = Box<dyn FnOnce(&mut Server, Response) + 'static>;

type Task = Box<dyn FnOnce() -> Event + Send + 'static>;

enum Event {
    Response(Response),
    Diagnostics {
        uri: Url,
        version: u64,
        diagnostics: Vec<Diagnostic>,
    },
    ClientExited,
    LoadFlake(Result<LoadFlakeResult>),
    NixosOptions(Result<NixosOptions>),
}

enum LoadFlakeResult {
    IsFlake {
        flake_info: FlakeInfo,
        missing_inputs: bool,
    },
    NotFlake,
}

enum ProtocolError {
    Send(mpsc::error::SendError<Message>),
    Recv,
}

impl From<mpsc::error::SendError<Message>> for ProtocolError {
    fn from(err: mpsc::error::SendError<Message>) -> Self {
        Self::Send(err)
    }
}

pub struct Server {
    // States.
    state: State,
    /// This contains an internal RWLock and must not lock together with `vfs`.
    host: AnalysisHost,
    vfs: Arc<RwLock<Vfs>>,
    opened_files: HashMap<Url, FileData>,
    config: Arc<Config>,
    /// Monotonic version counter for diagnostics calculation ordering.
    version_counter: u64,

    // Message passing.
    req_queue: ReqQueue<Infallible, ReqHandler>,
    lsp_tx: mpsc::Sender<Message>,
    lsp_rx: mpsc::Receiver<Message>,
    event_tx: mpsc::Sender<Event>,
    event_rx: mpsc::Receiver<Event>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    Uninitialized,
    Initializing,
    Running,
    ShuttingDown,
}

#[derive(Debug, Default)]
struct FileData {
    diagnostics_version: u64,
    diagnostics: Vec<Diagnostic>,
}

impl Server {
    pub fn new(lsp_tx: mpsc::Sender<Message>, lsp_rx: mpsc::Receiver<Message>) -> Self {
        // Arbitrary chosen size.
        let (event_tx, event_rx) = mpsc::channel(8);

        Self {
            state: State::Uninitialized,
            host: AnalysisHost::default(),
            vfs: Arc::new(RwLock::new(Vfs::new())),
            opened_files: HashMap::default(),
            // Will be initialized in `Server::run`.
            config: Arc::new(Config::new("/non-existing-path".into())),
            version_counter: 0,

            req_queue: ReqQueue::default(),
            lsp_tx,
            lsp_rx,
            event_tx,
            event_rx,
        }
    }

    pub async fn main_loop(mut self) -> Result<(), ProtocolError> {
        while let Some(msg) = self.lsp_rx.recv().await {
            match msg {
                Message::Notification(n) if n.method == notif::Exit::METHOD => return Ok(()),
                Message::Request(req) => self.dispatch_request(req).await,
                Message::Notification(n) => self.dispatch_notification(n).await,
                Message::Response(resp) => {
                    if let Some(cb) = self.req_queue.outgoing.complete(resp.id.clone()) {
                        cb(&mut self, resp);
                    }
                }
            }
        }

        Err(ProtocolError::Recv)
    }

    async fn init(&mut self, init_params: InitializeParams) -> Result<InitializeResult> {
        tracing::info!("Initialize: {init_params:?}");

        let root_path = match init_params
            .root_uri
            .as_ref()
            .and_then(|uri| uri.to_file_path().ok())
        {
            Some(path) => path,
            None => std::env::current_dir().expect("Failed to get the current directory"),
        };
        *Arc::get_mut(&mut self.config).expect("No concurrent access yet") = Config::new(root_path);

        // Client monitor to prevent outselves getting leaked.
        if let Some(pid) = init_params.process_id {
            let lsp_tx = self.lsp_tx.clone();
            tokio::spawn(async move {
                match wait_for_process(pid).await {
                    Ok(()) => {
                        // Use exit notification to force exit.
                        let _: Result<_, _> = lsp_tx
                            .send(Message::Notification(Notification {
                                method: notif::Exit::METHOD.into(),
                                params: Default::default(),
                            }))
                            .await;
                    }
                    Err(err) => {
                        tracing::warn!("Failed to monitor client process: {err}");
                    }
                }
            });
        }

        Ok(InitializeResult {
            capabilities: server_capabilities(),
            server_info: Some(ServerInfo {
                name: "nil".into(),
                version: option_env!("CFG_RELEASE").map(Into::into),
            }),
        })
    }

    async fn initialized(&mut self) {
        // Load configurations before loading flake.
        // The latter depends on `nix.binary`.
        self.load_config(|st| {
            // TODO: Register file watcher for flake.lock.
            st.load_flake();
        });
    }

    async fn dispatch_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Response(resp) => {
                todo!()
            }
            Event::Diagnostics {
                uri,
                version,
                diagnostics,
            } => match self.opened_files.get_mut(&uri) {
                Some(f) if f.diagnostics_version < version => {
                    f.diagnostics_version = version;
                    f.diagnostics = diagnostics.clone();
                    tracing::trace!(
                        "Push {} diagnostics of {uri}, version {version}",
                        diagnostics.len(),
                    );
                    self.send_notification::<notif::PublishDiagnostics>(PublishDiagnosticsParams {
                        uri,
                        diagnostics,
                        version: None,
                    });
                }
                _ => tracing::debug!("Ignore raced diagnostics of {uri}, version {version}"),
            },
            Event::ClientExited => {
                bail!("The process initializing this server is exited. Exit now")
            }
            Event::LoadFlake(ret) => match ret {
                Err(err) => {
                    self.show_message(
                        MessageType::ERROR,
                        format!("Failed to load flake workspace: {err:#}"),
                    );
                }
                Ok(LoadFlakeResult::IsFlake {
                    flake_info,
                    missing_inputs,
                }) => {
                    tracing::info!(
                        "Workspace is a flake (missing_inputs = {missing_inputs}): {flake_info:?}"
                    );
                    if missing_inputs {
                        self.show_message(MessageType::WARNING, "Some flake inputs are not available, please run `nix flake archive` to fetch all inputs");
                    }

                    // TODO: A better way to retrieve the nixpkgs for options?
                    if let Some(nixpkgs_path) = flake_info
                        .input_store_paths
                        .get(NIXOS_OPTIONS_FLAKE_INPUT)
                        .and_then(VfsPath::as_path)
                    {
                        let nixpkgs_path = nixpkgs_path.to_owned();
                        let nix_binary = self.config.nix_binary.clone();
                        tracing::info!("Evaluating NixOS options from {}", nixpkgs_path.display());
                        let event_tx = self.event_tx.clone();
                        tokio::task::spawn_blocking(move || {
                            let ret = nix_interop::nixos_options::eval_all_options(
                                &nix_binary,
                                &nixpkgs_path,
                            );
                            event_tx
                                .blocking_send(Event::NixosOptions(ret))
                                // No Debug.
                                .map_err(|_| ())
                                .expect("Channel closed");
                        });
                    }

                    self.vfs.write().unwrap().set_flake_info(Some(flake_info));
                    self.apply_vfs_change();
                }
                Ok(LoadFlakeResult::NotFlake) => {
                    tracing::info!("Workspace is not a flake");
                    self.vfs.write().unwrap().set_flake_info(None);
                    self.apply_vfs_change();
                }
            },
            Event::NixosOptions(ret) => match ret {
                // Sanity check.
                Ok(opts) if !opts.is_empty() => {
                    tracing::info!("Loaded NixOS options ({} top-level options)", opts.len());
                    self.vfs.write().unwrap().set_nixos_options(opts);
                    self.apply_vfs_change();
                }
                Ok(_) => {
                    tracing::error!("Empty NixOS options?");
                }
                Err(err) => {
                    tracing::error!("Failed to evalute NixOS options: {err}");
                }
            },
        }
        Ok(())
    }

    async fn dispatch_request(&mut self, req: Request) {
        let uninitialized = match self.state {
            State::Uninitialized => true,
            State::Initializing | State::Running => false,
            State::ShuttingDown => {
                let resp = Response::new_err(
                    req.id,
                    ErrorCode::InvalidRequest as i32,
                    "Server is shutting down".into(),
                );
                self.lsp_tx.send(resp.into()).await.expect("Channel closed");
                return;
            }
        };

        let state = self.state;
        let dispatcher = RequestDispatcher::Request(self, req);

        match state {
            State::Uninitialized => {
                return dispatcher
                    .on_mut::<req::Initialize, _>(Self::init)
                    .finish()
                    .await;
            }
            State::Initializing | State::ShuttingDown => return dispatcher.finish().await,
            State::Running => {}
        }

        dispatcher
            .on_mut::<req::Shutdown, _>(|this, ()| async {
                this.state = State::ShuttingDown;
                Ok(())
            })
            .on::<req::GotoDefinition>(handler::goto_definition)
            .on::<req::References>(handler::references)
            .on::<req::Completion>(handler::completion)
            .on::<req::SelectionRangeRequest>(handler::selection_range)
            .on::<req::PrepareRenameRequest>(handler::prepare_rename)
            .on::<req::Rename>(handler::rename)
            .on::<req::SemanticTokensFullRequest>(handler::semantic_token_full)
            .on::<req::SemanticTokensRangeRequest>(handler::semantic_token_range)
            .on::<req::HoverRequest>(handler::hover)
            .on::<req::DocumentSymbolRequest>(handler::document_symbol)
            .on::<req::Formatting>(handler::formatting)
            .on::<req::DocumentLinkRequest>(handler::document_links)
            .on::<req::CodeActionRequest>(handler::code_action)
            .on::<req::DocumentHighlightRequest>(handler::document_highlight)
            .on::<lsp_ext::ParentModule>(handler::parent_module)
            .finish();
    }

    async fn dispatch_notification(&mut self, notif: Notification) {
        NotificationDispatcher::Notification(self, notif)
            .on_mut::<notif::Cancel, _>(|st, params| async {
                let id: RequestId = match params.id {
                    NumberOrString::Number(id) => id.into(),
                    NumberOrString::String(id) => id.into(),
                };
                if let Some(resp) = st.req_queue.incoming.cancel(id) {
                    st.lsp_tx.send(resp.into()).unwrap();
                }
            })
            .on_mut::<notif::DidOpenTextDocument, _>(|st, params| async {
                // Ignore the open event for unsupported files, thus all following interactions
                // will error due to unopened files.
                let len = params.text_document.text.len();
                if len > MAX_FILE_LEN {
                    st.show_message(
                        MessageType::WARNING,
                        "Disable LSP functionalities for too large file ({len} > {MAX_FILE_LEN})",
                    );
                    return;
                }
                let uri = &params.text_document.uri;
                st.set_vfs_file_content(uri, params.text_document.text);
                st.opened_files.insert(uri.clone(), FileData::default());
            })
            .on_mut::<notif::DidCloseTextDocument, _>(|st, params| async {
                // N.B. Don't clear text here.
                st.opened_files.remove(&params.text_document.uri);
            })
            .on_mut::<notif::DidChangeTextDocument, _>(|st, params| async {
                let mut vfs = st.vfs.write().unwrap();
                let uri = &params.text_document.uri;
                // Ignore files not maintained in Vfs.
                let Ok(file) = vfs.file_for_uri(uri) else { return };
                for change in params.content_changes {
                    let ret = (|| {
                        let del_range = match change.range {
                            None => None,
                            Some(range) => Some(convert::from_range(&vfs, file, range).ok()?.1),
                        };
                        vfs.change_file_content(file, del_range, &change.text)
                            .ok()?;
                        Some(())
                    })();
                    if ret.is_none() {
                        tracing::error!(
                            "File is out of sync! Failed to apply change for {uri}: {change:?}"
                        );

                        // Clear file states to minimize pollution of the broken state.
                        st.opened_files.remove(uri);
                        // TODO: Remove the file from Vfs.
                    }
                }
                drop(vfs);
                st.apply_vfs_change();
            })
            .await
            // As stated in https://github.com/microsoft/language-server-protocol/issues/676,
            // this notification's parameters should be ignored and the actual config queried separately.
            .on_mut::<notif::DidChangeConfiguration, _>(|st, _params| async {
                st.load_config(|_| {});
            })
            .await
            // Workaround:
            // > In former implementations clients pushed file events without the server actively asking for it.
            // Ref: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#workspace_didChangeWatchedFiles
            .on_mut::<notif::DidChangeWatchedFiles, _>(|_st, _params| async {})
            .await
            .finish()
            .await;
    }

    /// Enqueue a task to reload the flake.{nix,lock} and the locked inputs.
    fn load_flake(&self) {
        tracing::info!("Loading flake configuration");

        let flake_path = self.config.root_path.join(FLAKE_FILE);
        let lock_path = self.config.root_path.join(FLAKE_LOCK_FILE);
        let nix_bin_path = self.config.nix_binary.clone();

        let vfs = self.vfs.clone();
        let task = move || {
            let flake_vpath = VfsPath::new(&flake_path);
            let flake_src = match fs::read_to_string(&flake_path) {
                Ok(src) => src,
                // Not a flake.
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return Ok(LoadFlakeResult::NotFlake);
                }
                // Read failure.
                Err(err) => {
                    return Err(anyhow::Error::new(err)
                        .context(format!("Failed to read flake root {flake_path:?}")));
                }
            };

            // Load the flake file in Vfs.
            let flake_file = {
                let mut vfs = vfs.write().unwrap();
                match vfs.file_for_path(&flake_vpath) {
                    // If the file is already opened (transferred from client),
                    // prefer the managed one. It contains more recent unsaved changes.
                    Ok(file) => file,
                    // Otherwise, cache the file content from disk.
                    Err(_) => vfs.set_path_content(flake_vpath, flake_src),
                }
            };

            let lock_src = match fs::read(&lock_path) {
                Ok(lock_src) => lock_src,
                // Flake without inputs.
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return Ok(LoadFlakeResult::IsFlake {
                        missing_inputs: false,
                        flake_info: FlakeInfo {
                            flake_file,
                            input_store_paths: HashMap::new(),
                        },
                    });
                }
                Err(err) => {
                    return Err(anyhow::Error::new(err)
                        .context(format!("Failed to read flake lock {lock_path:?}")));
                }
            };

            let inputs = flake_lock::resolve_flake_locked_inputs(&nix_bin_path, &lock_src)
                .context("Failed to resolve flake inputs from lock file")?;

            // We only need the map for input -> store path.
            let inputs_cnt = inputs.len();
            let input_store_paths = inputs
                .into_iter()
                .filter(|(_, input)| Path::new(&input.store_path).exists())
                .map(|(key, input)| (key, VfsPath::new(input.store_path)))
                .collect::<HashMap<_, _>>();

            Ok(LoadFlakeResult::IsFlake {
                missing_inputs: input_store_paths.len() != inputs_cnt,
                flake_info: FlakeInfo {
                    flake_file,
                    input_store_paths,
                },
            })
        };
        self.task_tx
            .send(Box::new(move || Event::LoadFlake(task())))
            .unwrap();
    }

    fn send_request<R: req::Request>(
        &mut self,
        params: R::Params,
        callback: impl FnOnce(&mut Self, Result<R::Result>) + 'static,
    ) {
        let callback = |this: &mut Self, resp: Response| {
            let ret = match resp.error {
                None => serde_json::from_value(resp.result.unwrap_or_default()).map_err(Into::into),
                Some(err) => Err(anyhow!(
                    "Request failed with {}: {}, data: {:?}",
                    err.code,
                    err.message,
                    err.data
                )),
            };
            callback(this, ret);
        };
        let req = self
            .req_queue
            .outgoing
            .register(R::METHOD.into(), params, Box::new(callback));
        self.lsp_tx.send(req.into()).unwrap();
    }

    fn send_notification<N: notif::Notification>(&self, params: N::Params) {
        self.lsp_tx
            .send(Notification::new(N::METHOD.into(), params).into())
            .unwrap();
    }

    // Maybe connect all tracing::* to LSP ShowMessage?
    fn show_message(&self, typ: MessageType, message: impl Into<String>) {
        let message = message.into();
        if typ == MessageType::ERROR {
            tracing::error!("{message}");
        }

        self.send_notification::<notif::ShowMessage>(ShowMessageParams { typ, message });
    }

    fn load_config(&mut self, callback: impl FnOnce(&mut Self) + 'static) {
        self.send_request::<req::WorkspaceConfiguration>(
            ConfigurationParams {
                items: vec![ConfigurationItem {
                    scope_uri: None,
                    section: Some(CONFIG_KEY.into()),
                }],
            },
            move |st, resp| {
                match resp {
                    Ok(mut v) => {
                        tracing::debug!("Updating config: {:?}", v);
                        st.update_config(v.pop().unwrap_or_default());
                    }
                    Err(err) => tracing::error!("Failed to update config: {}", err),
                }
                callback(st);
            },
        );
    }

    fn update_config(&mut self, value: serde_json::Value) {
        let mut config = Config::clone(&self.config);
        let (errors, updated_diagnostics) = config.update(value);
        tracing::debug!("Updated config, errors: {errors:?}, config: {config:?}");
        self.config = Arc::new(config);

        if !errors.is_empty() {
            let msg = ["Failed to apply some settings:"]
                .into_iter()
                .chain(errors.iter().flat_map(|s| ["\n- ", s]))
                .collect::<String>();
            self.show_message(MessageType::ERROR, msg);
        }

        // Refresh all diagnostics since the filter may be changed.
        if updated_diagnostics {
            let version = self.next_version();
            for uri in self.opened_files.keys() {
                tracing::trace!("Recalculate diagnostics of {uri}, version {version}");
                self.update_diagnostics(uri.clone(), version);
            }
        }
    }

    fn update_diagnostics(&self, uri: Url, version: u64) {
        let snap = self.snapshot();
        let task = move || {
            // Return empty diagnostics for ignored files.
            let diagnostics = (!snap.config.diagnostics_excluded_files.contains(&uri))
                .then(|| {
                    with_catch_unwind("diagnostics", || handler::diagnostics(snap, &uri))
                        .unwrap_or_else(|err| {
                            tracing::error!("Failed to calculate diagnostics: {err}");
                            Vec::new()
                        })
                })
                .unwrap_or_default();
            Event::Diagnostics {
                uri,
                version,
                diagnostics,
            }
        };
        self.task_tx.send(Box::new(task)).unwrap();
    }

    fn next_version(&mut self) -> u64 {
        self.version_counter += 1;
        self.version_counter
    }

    fn snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            analysis: self.host.snapshot(),
            vfs: Arc::clone(&self.vfs),
            config: Arc::clone(&self.config),
        }
    }

    fn set_vfs_file_content(&mut self, uri: &Url, text: String) {
        let vpath = uri.to_vfs_path();
        self.vfs.write().unwrap().set_path_content(vpath, text);
        self.apply_vfs_change();
    }

    fn apply_vfs_change(&mut self) {
        let changes = self.vfs.write().unwrap().take_change();
        tracing::trace!("Change: {:?}", changes);
        let file_changes = changes.file_changes.clone();

        // N.B. This acquires the internal write lock.
        // Must be called without holding the lock of `vfs`.
        self.host.apply_change(changes);

        let version = self.next_version();
        let vfs = self.vfs.read().unwrap();
        for (file, text) in file_changes {
            let uri = vfs.uri_for_file(file);
            if !self.opened_files.contains_key(&uri) {
                continue;
            }

            // FIXME: Removed or closed files are indistinguishable from empty files.
            if !text.is_empty() {
                self.update_diagnostics(uri, version);
            } else {
                // Clear diagnostics.
                self.event_tx
                    .send(Event::Diagnostics {
                        uri,
                        version,
                        diagnostics: Vec::new(),
                    })
                    .await
                    .unwrap();
            }
        }
    }
}

#[must_use = "RequestDispatcher::finish not called"]
enum RequestDispatcher<'s> {
    Request(&'s mut Server, Request),
    Dispatched(Box<dyn Future<Output = ()> + 's>),
}

impl<'s> RequestDispatcher<'s> {
    fn on_mut<R, Fut>(mut self, f: fn(&mut Server, R::Params) -> Fut) -> Self
    where
        R: req::Request,
        Fut: Future<Output = Result<R::Result>>,
    {
        match self {
            Self::Request(server, req) if req.method == R::METHOD => {
                Self::Dispatched(Box::new(async {
                    let ret = async {
                        let params = serde_json::from_value::<R::Params>(req.params)?;
                        let ret = f(server, params).await?;
                        Ok(serde_json::to_value(ret).unwrap())
                    }
                    .await;
                    let resp = result_to_response(req.id, ret);
                    server
                        .lsp_tx
                        .send(resp.into())
                        .await
                        .expect("Channel closed");
                }))
            }
            _ => self,
        }
    }

    fn on<R>(mut self, f: fn(StateSnapshot, R::Params) -> Result<R::Result>) -> Self
    where
        R: req::Request,
        R::Params: 'static,
        R::Result: 'static,
    {
        match self {
            Self::Request(server, req) if req.method == R::METHOD => {
                Self::Dispatched(Box::new(async {
                    let snap = server.snapshot();
                    let lsp_tx = server.lsp_tx.clone();
                    tokio::task::spawn_blocking(move || {
                        let ret = with_catch_unwind(R::METHOD, || {
                            let params = serde_json::from_value::<R::Params>(req.params)?;
                            let resp = f(snap, params)?;
                            Ok(serde_json::to_value(resp)?)
                        });
                        let resp = result_to_response(req.id, ret);
                        lsp_tx.blocking_send(resp.into()).expect("Channel closed");
                    });
                }))
            }
            _ => self,
        }
    }

    async fn finish(self) {
        match self {
            RequestDispatcher::Request(server, req) => {
                let resp = Response::new_err(req.id, ErrorCode::MethodNotFound as _, String::new());
                server
                    .lsp_tx
                    .send(resp.into())
                    .await
                    .expect("Channel closed");
            }
            RequestDispatcher::Dispatched(fut) => Box::into_pin(fut).await,
        }
    }
}

#[must_use = "NotificationDispatcher::finish not called"]
enum NotificationDispatcher<'s> {
    Notification(&'s mut Server, Notification),
    Dispatched(Box<dyn Future<Output = ()> + 's>),
}

impl<'s> NotificationDispatcher<'s> {
    async fn on_mut<N, Fut>(
        self,
        f: fn(&mut Server, N::Params) -> Fut,
    ) -> NotificationDispatcher<'s>
    where
        N: notif::Notification,
        N::Params: 's,
        Fut: Future<Output = ()> + 's,
    {
        match self {
            Self::Notification(server, n) if n.method == N::METHOD => {
                Self::Dispatched(Box::new(async move {
                    match serde_json::from_value::<N::Params>(n.params) {
                        Ok(params) => f(server, params).await,
                        Err(err) => {
                            tracing::error!("Failed to parse notification {}: {}", N::METHOD, err)
                        }
                    }
                }))
            }
            _ => self,
        }
    }

    async fn finish(self) {
        match self {
            Self::Notification(_, n) => {
                if !n.method.starts_with("$/") {
                    tracing::error!("Unhandled notification: {:?}", n);
                }
            }
            Self::Dispatched(fut) => Box::into_pin(fut).await,
        }
    }
}

fn with_catch_unwind<T>(ctx: &str, f: impl FnOnce() -> Result<T> + UnwindSafe) -> Result<T> {
    static INSTALL_PANIC_HOOK: Once = Once::new();
    thread_local! {
        static PANIC_LOCATION: Cell<String> = Cell::new(String::new());
    }

    INSTALL_PANIC_HOOK.call_once(|| {
        let old_hook = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let loc = info
                .location()
                .map(|loc| loc.to_string())
                .unwrap_or_default();
            let backtrace = Backtrace::force_capture();
            PANIC_LOCATION.with(|inner| {
                inner.set(format!("Location: {loc:#}\nBacktrace: {backtrace:#}"));
            });
            old_hook(info);
        }));
    });

    match panic::catch_unwind(f) {
        Ok(ret) => ret,
        Err(payload) => {
            let reason = payload
                .downcast_ref::<String>()
                .map(|s| &**s)
                .or_else(|| payload.downcast_ref::<&str>().map(|s| &**s))
                .unwrap_or("unknown");
            let mut loc = PANIC_LOCATION.with(|inner| inner.take());
            if loc.is_empty() {
                loc = "Location: unknown".into();
            }
            tracing::error!("Panicked in {ctx}: {reason}\n{loc}");
            bail!("Panicked in {ctx}: {reason}\n{loc}");
        }
    }
}

fn result_to_response(id: RequestId, ret: Result<serde_json::Value>) -> Response {
    let err = match ret {
        Ok(v) => {
            return Response {
                id,
                result: Some(v),
                error: None,
            }
        }
        Err(err) => err,
    };

    if err.is::<Cancelled>() {
        // When client cancelled a request, a response is immediately sent back,
        // and the response will be ignored.
        return Response::new_err(id, ErrorCode::ServerCancelled as i32, "Cancelled".into());
    }
    if let Some(err) = err.downcast_ref::<LspError>() {
        return Response::new_err(id, err.code as i32, err.to_string());
    }
    if let Some(err) = err.downcast_ref::<serde_json::Error>() {
        return Response::new_err(id, ErrorCode::InvalidParams as i32, err.to_string());
    }
    Response::new_err(id, ErrorCode::InternalError as i32, err.to_string())
}

#[derive(Debug)]
pub struct StateSnapshot {
    pub(crate) analysis: Analysis,
    vfs: Arc<RwLock<Vfs>>,
    pub(crate) config: Arc<Config>,
}

impl StateSnapshot {
    pub(crate) fn vfs(&self) -> impl std::ops::Deref<Target = Vfs> + '_ {
        self.vfs.read().unwrap()
    }
}

#[cfg(target_os = "linux")]
async fn wait_for_process(pid: u32) -> Result<()> {
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::ptr::null_mut;

    fn wait_remote_pid(pid: libc::pid_t) -> Result<(), io::Error> {
        let pidfd = unsafe {
            let ret = libc::syscall(libc::SYS_pidfd_open, pid, 0 as libc::c_int);
            if ret == -1 {
                return Err(io::Error::last_os_error());
            }
            OwnedFd::from_raw_fd(ret as RawFd)
        };
        unsafe {
            let mut fdset = MaybeUninit::uninit();
            libc::FD_ZERO(fdset.as_mut_ptr());
            libc::FD_SET(pidfd.as_raw_fd(), fdset.as_mut_ptr());
            let nfds = pidfd.as_raw_fd() + 1;
            let ret = libc::select(nfds, fdset.as_mut_ptr(), null_mut(), null_mut(), null_mut());
            if ret == -1 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    tokio::task::spawn_blocking(move || match wait_remote_pid(pid as _) {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        Err(err) => return Err(err.into()),
    })
    .await
    .context("Failed to spawn task")?
}

#[cfg(not(target_os = "linux"))]
async fn wait_for_process(_pid: u32) -> Result<()> {
    bail!("Waiting for arbitrary PID is not supported on this platform");
}
