package impl

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
)

func CreateWatchDir(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, watchDirFile string, urlFlag string) {

	logger.Info("CreateWatchDir implementation here", zap.String("serviceCrt", serviceCrt), zap.String("watchDirFile", watchDirFile))

	client, err := GetServiceConnection(logger, serviceCrt, urlFlag)
	if err != nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}
	if client == nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}
	req := pb.CreateWatchDirectoryRequest{}
	content, err := ioutil.ReadFile(watchDirFile)
	if err != nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}

	req.WatchdirString = string(content)
	resp, err := client.CreateWatchDirectory(context.Background(), &req)
	if err != nil {
		logger.Errorf("error in CreateWatchDirectoryFunction %s\n", err.Error())
		os.Exit(1)
	}
	logger.Infof("response %s\n", fmt.Sprintf("%v", resp))

}

func DeleteWatchDir(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, watchDirId int, urlFlag string) {

}

func GetWatchDir(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, urlFlag string) {

}
