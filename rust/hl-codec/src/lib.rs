//! Hyperliquid 热路径编解码与签名（Phase 4 实现）。
//!
//! 设计约束见 `docs/adr/0001-rust-hotpath-ffi.md`：
//! - 只实现无状态纯函数（输入 bytes → 输出 bytes）；
//! - 不持有跨调用状态、不做 I/O、不 panic 穿过 FFI 边界；
//! - 错误通过返回码传出，调用方（Go）负责映射为 error。
//!
//! Phase 4 计划导出：
//! - `hl_abi_version() -> u32`
//! - `hl_sign_l1_action(action_ptr, action_len, nonce, key_ptr, out_ptr) -> i32`
//! - `hl_encode_order_action(...) -> i32`

/// ABI 版本。Go 侧启动时校验，不匹配必须 fail-fast。
#[no_mangle]
pub extern "C" fn hl_abi_version() -> u32 {
    1
}

/// 骨架期占位：返回 -1（未实现），Phase 4 替换为真实实现。
#[no_mangle]
pub extern "C" fn hl_sign_l1_action(
    _action_ptr: *const u8,
    _action_len: usize,
    _nonce: i64,
    _key_ptr: *const u8,
    _out_ptr: *mut u8,
) -> i32 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_version_is_positive() {
        assert!(hl_abi_version() >= 1);
    }
}
