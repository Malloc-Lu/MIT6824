package raft

import "go.uber.org/zap"

func main() {
	logger := zap.NewExample().Sugar()
	defer logger.Sync()

	s := "it can be structed data"
	logger.Info("start to use zap: %s", s)
}