// Command controld 是独立于策略进程的控制面。
//
// 存在的唯一理由：策略进程 panic/OOM/死循环时，"全平 + 停机"仍必须能执行。
// Phase 2 计划：unix socket + 本地文件标志双通道，暴露
//
//	start / stop / flatten / status / set <key>=<value>
//
// 并写入 /run/hft/killswitch 供 exec 层每 100ms 轮询。
package main

import "fmt"

func main() {
	fmt.Println("controld: Phase 1 骨架（未实现）")
	fmt.Println("计划接口: unix socket /run/hft/controld.sock + /run/hft/killswitch 文件标志")
}
