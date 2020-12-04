// Package backpressure provides logic for handling backpressure as defined
// within churro.  Backpressure is when a target requires the sender
// to shut off sending messages to it.  The protocol is simple with a
// 1 indicating to apply backpressure, or 0 if no backpressure exists.
package backpressure

import (
	"go.uber.org/zap"
)

// CheckBackpressure checks to see if a queue is at the headroom
// capacity.  It returns either a 1 or 0 to indicate a backpressure.
func CheckBackpressure(currentQueueSize, maxQueueSize, pctHeadRoom int, logger *zap.SugaredLogger) int32 {
	headRoomPoint := int(.01 * float64(pctHeadRoom) * float64(maxQueueSize))
	if currentQueueSize >= headRoomPoint {
		logger.Infof("backpressure is turning on %d %d\n", headRoomPoint, currentQueueSize)
		return 1
	}
	return 0
}
