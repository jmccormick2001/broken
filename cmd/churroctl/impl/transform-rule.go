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

func CreateTransformRule(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, transformRuleFile string, urlFlag string) {

	logger.Infof("CreateTransformRule implementation here %s %s\n", serviceCrt, transformRuleFile)

	client, err := GetServiceConnection(logger, serviceCrt, urlFlag)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	if client == nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	req := pb.CreateTransformRuleRequest{}
	content, err := ioutil.ReadFile(transformRuleFile)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	req.RuleString = string(content)
	resp, err := client.CreateTransformRule(context.Background(), &req)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	logger.Infof("response %s", fmt.Sprintf("%v", resp))

}

func DeleteTransformRule(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, transformRuleId int, urlFlag string) {

}

func GetTransformRule(logger *zap.SugaredLogger, cmd *cobra.Command, args []string, serviceCrt string, urlFlag string) {

}
