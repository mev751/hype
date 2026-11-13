// Command hftd 是 HFT 平台主进程。
//
// Phase 1：仅有骨架，业务逻辑尚未实现（接口见 internal/hft，设计见 docs/architecture.md）。
// Phase 2 目标：feed → book → strategy → risk → exec → reconcile 全链路打通，
// 并把现有网格策略迁移为第一个 strategy.Strategy 实现。
package main

import (
	"fmt"
	"os"

	"github.com/mev751/hype/internal/hft/hotpath"
)

func main() {
	fmt.Println("hftd: Phase 1 骨架（未实现业务逻辑）")
	fmt.Printf("hotpath backend=%s\n", hotpathBackendName())

	// 显式返回 0：骨架阶段允许被 CI/部署脚本调用但不产生交易副作用
	os.Exit(0)
}

func hotpathBackendName() string {
	s, err := hotpath.NewSigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		return "unavailable"
	}
	return s.Backend()
}
