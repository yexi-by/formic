//! 验证用工具：以新进程组启动给定命令，等待指定毫秒后向其发送终止事件
//! （CTRL_BREAK_EVENT；新进程组的 CTRL+C 默认禁用而 BREAK 恒启用）。
//!
//! 用法：`send_ctrlc <延迟毫秒> <命令> [参数...]`

#[cfg(windows)]
fn main() {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    // CREATE_NEW_PROCESS_GROUP 下 CTRL+C 默认禁用，CTRL+BREAK 恒启用
    const CTRL_BREAK_EVENT: u32 = 1;

    unsafe extern "C" {
        fn GenerateConsoleCtrlEvent(event: u32, process_group_id: u32) -> i32;
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    let delay: u64 = args
        .first()
        .and_then(|s| s.parse().ok())
        .expect("第一个参数是延迟毫秒");
    let command = args.get(1).expect("第二个参数是要启动的命令");

    let mut child = std::process::Command::new(command)
        .args(&args[2..])
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .expect("启动子进程失败");

    std::thread::sleep(std::time::Duration::from_millis(delay));
    let ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, child.id()) };
    println!("CTRL_BREAK_EVENT 已发送（ok={ok}），等待子进程收敛…");

    let status = child.wait().expect("等待子进程失败");
    println!("子进程退出码：{:?}", status.code());
}

#[cfg(not(windows))]
fn main() {
    eprintln!("send_ctrlc 仅用于 Windows 验证（GenerateConsoleCtrlEvent）");
    std::process::exit(2);
}
