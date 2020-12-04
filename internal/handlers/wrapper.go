package handlers

import "go.uber.org/zap"

type HandlerWrapper struct {
	ErrorText string
	Log       *zap.SugaredLogger
}
