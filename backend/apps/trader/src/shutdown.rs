use std::io;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownReason {
    Interrupt,
    Terminate,
}

#[cfg(unix)]
pub struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    pub fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    pub async fn wait(&mut self) -> io::Result<ShutdownReason> {
        tokio::select! {
            signal = self.interrupt.recv() => received(signal, ShutdownReason::Interrupt),
            signal = self.terminate.recv() => received(signal, ShutdownReason::Terminate),
        }
    }
}

#[cfg(unix)]
fn received(signal: Option<()>, reason: ShutdownReason) -> io::Result<ShutdownReason> {
    signal
        .map(|()| reason)
        .ok_or_else(|| io::Error::other("operating-system signal stream closed unexpectedly"))
}

#[cfg(not(unix))]
pub struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    pub fn install() -> io::Result<Self> {
        Ok(Self)
    }

    pub async fn wait(&mut self) -> io::Result<ShutdownReason> {
        tokio::signal::ctrl_c().await?;
        Ok(ShutdownReason::Interrupt)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Command;
    use std::time::Duration;

    use super::{ShutdownReason, ShutdownSignals};

    #[tokio::test]
    async fn sigterm_requests_graceful_shutdown() {
        let mut signals = ShutdownSignals::install().expect("signals should install");
        let process_id = std::process::id().to_string();

        let result = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(signals.wait(), async {
                tokio::task::yield_now().await;
                Command::new("kill")
                    .args(["-TERM", &process_id])
                    .status()
                    .expect("kill command should run")
            })
        })
        .await
        .expect("SIGTERM should be received before the timeout");

        assert!(result.1.success());
        assert_eq!(
            result.0.expect("signal should be received"),
            ShutdownReason::Terminate
        );
    }
}
