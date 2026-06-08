//! Android Storage Access Framework (SAF) file opener via JNI.
//!
//! Uses `ContentResolver.openFileDescriptor()` to convert a `content://`
//! URI into a raw file descriptor that Rust can mmap / read directly.
//!
//! JVM discovery: Kotlin `MainActivity` calls `System.loadLibrary` to
//! register our native library with the JVM, then calls the
//! `initializeRustJni` native method which captures the `JavaVM`
//! reference for later use by SAF functions.

use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::PathBuf;
use std::sync::OnceLock;

use jni::objects::JValueGen;
use jni::{JNIEnv, JavaVM};

// ─── JVM storage ────────────────────────────────────────────────────

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_localsend_localsend_1app_MainActivity_initializeRustJni(
    env: JNIEnv<'_>,
    _class: jni::objects::JClass<'_>,
) {
    match env.get_java_vm() {
        Ok(vm) => {
            let _ = JAVA_VM.set(vm);
        }
        Err(e) => {
            eprintln!("initializeRustJni: get_java_vm failed: {e}");
        }
    }
}

fn get_java_vm() -> Result<&'static JavaVM, String> {
    JAVA_VM.get().ok_or_else(|| {
        "JVM not initialized — initializeRustJni not called from Kotlin".to_string()
    })
}

// ─── Public API ─────────────────────────────────────────────────────

/// Open a `content://` URI for reading via Android's ContentResolver.
pub fn open_content_uri(uri: &str) -> Result<File, String> {
    let vm = get_java_vm()?;
    let mut env = vm
        .attach_current_thread()
        .map_err(|e| format!("JNI: failed to attach thread: {e}"))?;

    let context = get_application_context(&mut env)?;

    let content_resolver = env
        .call_method(
            &context,
            "getContentResolver",
            "()Landroid/content/ContentResolver;",
            &[],
        )
        .map_err(|e| format!("JNI: getContentResolver: {e}"))?
        .l()
        .map_err(|e| format!("JNI: getContentResolver returned non-object: {e}"))?;

    let uri_str = env
        .new_string(uri)
        .map_err(|e| format!("JNI: new_string: {e}"))?;
    let uri_obj = env
        .call_static_method(
            "android/net/Uri",
            "parse",
            "(Ljava/lang/String;)Landroid/net/Uri;",
            &[JValueGen::Object(&uri_str)],
        )
        .map_err(|e| format!("JNI: Uri.parse: {e}"))?
        .l()
        .map_err(|e| format!("JNI: Uri.parse returned non-object: {e}"))?;

    let mode_str = env
        .new_string("r")
        .map_err(|e| format!("JNI: new_string: {e}"))?;
    let parcel_fd = env
        .call_method(
            &content_resolver,
            "openFileDescriptor",
            "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;",
            &[JValueGen::Object(&uri_obj), JValueGen::Object(&mode_str)],
        )
        .map_err(|e| format!("JNI: openFileDescriptor({uri}): {e}"))?
        .l()
        .map_err(|e| format!("JNI: openFileDescriptor returned non-object: {e}"))?;

    let fd = env
        .call_method(&parcel_fd, "detachFd", "()I", &[])
        .map_err(|e| format!("JNI: detachFd: {e}"))?
        .i()
        .map_err(|e| format!("JNI: detachFd returned non-int: {e}"))?;

    if fd < 0 {
        return Err(format!("JNI: detachFd returned invalid fd: {fd}"));
    }

    Ok(unsafe { File::from_raw_fd(fd) })
}

// ─── Cross-platform file access helpers ─────────────────────────────

/// Open a `content://` URI and return a filesystem path that iroh can use
/// for zero-copy import via mmap.
///
/// On Android (Linux), this creates a `/proc/self/fd/N` symlink path from
/// the SAF file descriptor. iroh's `add_path` with `TryReference` will
/// mmap this path directly — no full-file copy into memory.
///
/// The returned path is only valid while the underlying fd is open.
/// The caller must keep the `File` alive for the duration of the import.
pub fn open_uri_as_path(uri: &str) -> Result<(PathBuf, File), String> {
    let file = open_content_uri(uri)?;
    let fd = file.as_raw_fd();
    let path = PathBuf::from(format!("/proc/self/fd/{fd}"));
    Ok((path, file))
}

/// Open a `content://` URI and read its entire contents into Bytes.
///
/// Prefer `open_uri_as_path` for large files — this loads everything
/// into memory and should only be used for small files or as a fallback.
pub fn read_uri_to_bytes(uri: &str) -> Result<bytes::Bytes, String> {
    use std::io::Read;
    let mut file = open_content_uri(uri)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    Ok(buf.into())
}

// ─── Helpers ────────────────────────────────────────────────────────

fn get_application_context<'local>(
    env: &mut JNIEnv<'local>,
) -> Result<jni::objects::JObject<'local>, String> {
    let activity_thread_class = env
        .find_class("android/app/ActivityThread")
        .map_err(|e| format!("JNI: find ActivityThread: {e}"))?;

    let current_at = env
        .call_static_method(
            &activity_thread_class,
            "currentActivityThread",
            "()Landroid/app/ActivityThread;",
            &[],
        )
        .map_err(|e| format!("JNI: currentActivityThread: {e}"))?
        .l()
        .map_err(|e| format!("JNI: currentActivityThread non-object: {e}"))?;

    let app = env
        .call_method(
            &current_at,
            "getApplication",
            "()Landroid/app/Application;",
            &[],
        )
        .map_err(|e| format!("JNI: getApplication: {e}"))?
        .l()
        .map_err(|e| format!("JNI: getApplication non-object: {e}"))?;

    Ok(app)
}
