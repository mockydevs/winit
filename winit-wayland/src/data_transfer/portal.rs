//! Resolve a portal capability after Drop, without blocking the event loop.
use std::cell::RefCell;
use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use calloop::channel;

use super::{DataFetch, MimeType};
use crate::state::WinitState;
static WORKERS: AtomicUsize = AtomicUsize::new(0);
struct Worker;
impl Drop for Worker {
    fn drop(&mut self) {
        WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}
fn key(bytes: &[u8]) -> Option<&str> {
    if bytes.is_empty() || bytes.len() > 4096 {
        return None;
    }
    // GTK serializes a string including its trailing C terminator; other sources omit it.
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    let key = std::str::from_utf8(bytes).ok()?;
    // GTK sends the key as UTF-8, with no URI decoding or shell interpretation.
    (!key.is_empty() && !key.contains('\0') && !key.chars().any(char::is_control)).then_some(key)
}

fn retrieve(key: &str) -> Option<Vec<PathBuf>> {
    let connection = dbus::blocking::Connection::new_session().ok()?;
    let proxy = connection.with_proxy(
        "org.freedesktop.portal.Documents",
        "/org/freedesktop/portal/documents",
        Duration::from_secs(5),
    );
    let (paths,): (Vec<String>,) = proxy
        .method_call(
            "org.freedesktop.portal.FileTransfer",
            "RetrieveFiles",
            (key, dbus::arg::PropMap::new()),
        )
        .ok()?;
    if paths.is_empty()
        || paths.len() > 256
        || paths.iter().map(String::len).sum::<usize>() > 1024 * 1024
    {
        return None;
    }
    paths
        .into_iter()
        .map(|p| {
            let path = PathBuf::from(&p);
            (path.is_absolute() && !p.contains('\0')).then_some(path)
        })
        .collect()
}

fn fail(fetch: &Rc<RefCell<DataFetch>>, state: &mut WinitState) {
    fetch
        .borrow_mut()
        .complete(Err(Arc::new(io::Error::other("file portal transfer failed"))), state);
}
pub(super) fn resolve(fetch: Rc<RefCell<DataFetch>>, bytes: Vec<u8>, state: &mut WinitState) {
    let Some(key) = key(&bytes).map(str::to_owned) else {
        fail(&fetch, state);
        return;
    };
    if WORKERS
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < 8).then_some(n + 1))
        .is_err()
    {
        fail(&fetch, state);
        return;
    }
    let worker = Worker;
    let (sender, receiver) = channel::sync_channel::<Option<Vec<u8>>>(1);
    let receive = Rc::clone(&fetch);
    let token = match state.loop_handle.insert_source(receiver, move |event, _, state| {
        if let channel::Event::Msg(paths) = event {
            let mut fetch = receive.borrow_mut();
            if fetch.mime_type.is_none() {
                return;
            }
            let result =
                paths.ok_or_else(|| Arc::new(io::Error::other("file portal transfer failed")));
            fetch.mime_type = Some(MimeType::parse("text/uri-list".into()));
            fetch.complete(result, state);
        }
    }) {
        Ok(token) => token,
        Err(_) => {
            fail(&fetch, state);
            return;
        },
    };
    fetch.borrow_mut().portal_channel = Some(token);
    if std::thread::Builder::new()
        .name("wayland-file-portal".into())
        .spawn(move || {
            let _worker = worker;
            let paths = retrieve(&key).and_then(|paths| {
                let uris = paths
                    .into_iter()
                    .map(|p| url::Url::from_file_path(p).ok().map(|uri| uri.to_string()))
                    .collect::<Option<Vec<_>>>()?;
                let bytes = uris.join("\r\n").into_bytes();
                (bytes.len() <= 1024 * 1024).then_some(bytes)
            });
            let _ = sender.send(paths);
        })
        .is_err()
    {
        fail(&fetch, state);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn portal_capabilities_are_literal_and_bounded() {
        assert_eq!(super::key(b"123-abc"), Some("123-abc"));
        assert_eq!(super::key(b"123-abc\0"), Some("123-abc"));
        for invalid in [b"".as_slice(), b"\0", b"a\0b", b"abc\0\0", b"a\nb", &[255]] {
            assert!(super::key(invalid).is_none());
        }
        assert!(super::key(&vec![b'a'; 4097]).is_none());
    }
}
