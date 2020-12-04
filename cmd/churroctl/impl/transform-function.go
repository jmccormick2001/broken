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

func CreateTransformFunction(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, transformFunctionFile string, urlFlag string) {

	logger.Infof("CreateTransformFunction implementation here %s %s\n", serviceCrt, transformFunctionFile)
	client, err := GetServiceConnection(logger, serviceCrt, urlFlag)
	if err != nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}
	if client == nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}
	req := pb.CreateTransformFunctionRequest{}
	content, err := ioutil.ReadFile(transformFunctionFile)
	if err != nil {
		logger.Errorf("error in getServiceConnection %s\n", err.Error())
		os.Exit(1)
	}

	req.FunctionString = string(content)
	resp, err := client.CreateTransformFunction(context.Background(), &req)
	if err != nil {
		logger.Errorf("error in CreateTransformFunction %s\n", err.Error())
		os.Exit(1)
	}
	logger.Infof("response %s\n", fmt.Sprintf("%v", resp))

}

func DeleteTransformFunction(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, transformFunctionId int, urlFlag string) {

	logger.Infof("DeleteTransformFunction implementation here %s %d\n", serviceCrt, transformFunctionId)
}

func GetTransformFunction(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, urlFlag string) {

	logger.Infof("GetTransformFunction implementation here %s\n", serviceCrt)

}
