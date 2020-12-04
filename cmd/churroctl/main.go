package main

import (
	"context"

	"fmt"

	"os"

	"github.com/spf13/cobra"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/cmd/churroctl/impl"
	"gitlab.com/churro-group/churro/internal"
	"gitlab.com/churro-group/churro/internal/pipeline"
	"go.uber.org/zap"
	"k8s.io/client-go/rest"

	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

var transformFunctionFile string
var transformFunctionId int
var transformRuleFile string
var transformRuleId int
var watchDirFile, serviceCrt string
var watchDirId int
var namespace string
var crPath string
var urlFlag string
var adminUser string

var logger *zap.SugaredLogger

func main() {

	l, _ := zap.NewDevelopment()
	logger = l.Sugar()
	logger.Info("churroctl")

	// top level commands
	createCmd := createCommand()
	deleteCmd := deleteCommand()
	getCmd := getCommand()

	// pipeline subcommands
	createPipelineCmd := createPipelineCommand()
	deletePipelineCmd := deletePipelineCommand()
	getPipelineCmd := getPipelineCommand()
	getPipelineCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")

	// watchdirectory subcommands
	createWatchDirCmd := createWatchDirCommand()
	createWatchDirCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")
	createWatchDirCmd.PersistentFlags().StringVarP(&watchDirFile, "watchdirfile", "f", "", "watch directory yaml file")

	getWatchDirCmd := getWatchDirCommand()
	getWatchDirCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")

	deleteWatchDirCmd := deleteWatchDirCommand()
	deleteWatchDirCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "serivce certificate for a given pipeline")
	deleteWatchDirCmd.PersistentFlags().IntVarP(&watchDirId, "watchdirid", "i", 0, "watch directory id, integer value")

	// transform function subcommands
	createTransformFunctionCmd := createTransformFunctionCommand()
	createTransformFunctionCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")
	createTransformFunctionCmd.PersistentFlags().StringVarP(&transformFunctionFile, "transformfunctionfile", "f", "", "transformfunction file yaml file")

	getTransformFunctionCmd := getTransformFunctionCommand()
	getTransformFunctionCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")

	deleteTransformFunctionCmd := deleteTransformFunctionCommand()
	deleteTransformFunctionCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "serivce certificate for a given pipeline")
	deleteTransformFunctionCmd.PersistentFlags().IntVarP(&transformFunctionId, "transformfunctionid", "i", 0, "transformfunction id, integer value")

	// transform rule subcommands
	createTransformRuleCmd := createTransformRuleCommand()
	createTransformRuleCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")
	createTransformRuleCmd.PersistentFlags().StringVarP(&transformRuleFile, "transformrulefile", "f", "", "transformrule file yaml file")

	getTransformRuleCmd := getTransformRuleCommand()
	getTransformRuleCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "service certificate for a given pipeline")

	deleteTransformRuleCmd := deleteTransformRuleCommand()
	deleteTransformRuleCmd.PersistentFlags().StringVarP(&serviceCrt, "servicecrt", "s", "", "serivce certificate for a given pipeline")
	deleteTransformRuleCmd.PersistentFlags().IntVarP(&transformRuleId, "transformruleid", "i", 0, "transformrule id, integer value")

	createCmd.AddCommand(createPipelineCmd)
	createCmd.AddCommand(createWatchDirCmd)
	createCmd.AddCommand(createTransformFunctionCmd)
	createCmd.AddCommand(createTransformRuleCmd)

	deleteCmd.AddCommand(deletePipelineCmd)
	deleteCmd.AddCommand(deleteWatchDirCmd)
	deleteCmd.AddCommand(deleteTransformFunctionCmd)
	deleteCmd.AddCommand(deleteTransformRuleCmd)

	getCmd.AddCommand(getPipelineCmd)
	getCmd.AddCommand(getWatchDirCmd)
	getCmd.AddCommand(getTransformFunctionCmd)
	getCmd.AddCommand(getTransformRuleCmd)

	// root command
	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "default", "namespace")
	//	rootCmd.PersistentFlags().StringVarP(&crPath, "cr", "c", "", "custom resource file path, only for creating a pipeline")
	rootCmd.PersistentFlags().StringVarP(&urlFlag, "url", "x", "localhost:8088", "churro-ctl url")

	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(getCmd)

	createCmd.PersistentFlags().StringVarP(&adminUser, "adminuser", "u", "root", "database admin userid")

	if err := rootCmd.Execute(); err != nil {
		logger.Errorf("error in execute %s\n", err.Error())
	}

}

func getPipelineCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			logger.Infof("getPipelineCommand %s\n", serviceCrt)
			client, err := impl.GetServiceConnection(logger, serviceCrt, urlFlag)
			if err != nil {
				logger.Errorf("error in getServiceConnection %s\n", err.Error())
				os.Exit(1)
			}
			req := pb.GetPipelineRequest{}
			resp, err := client.GetPipeline(context.Background(), &req)
			if err != nil {
				logger.Errorf("error in ShowPipeline", err.Error())
				os.Exit(1)
			}
			logger.Infof("response %s\n", fmt.Sprintf("%v", resp))

		},
		Use:   `pipeline`,
		Short: "Get pipeline info",
		Long:  "This is a command that gets pipeline info",
	}

	return cmd
}

func createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
		},
		Use:   `create`,
		Short: "Create churro resouces",
		Args:  cobra.MinimumNArgs(1),
		Long:  "This is a command that creates churro resources",
	}

	return cmd
}

func deleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
		},
		Use:   `delete`,
		Short: "Delete churro resources",
		Args:  cobra.MinimumNArgs(1),
		Long:  "This is a command that deletes churro resources",
	}

	return cmd
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
		},
		Use:   `get`,
		Short: "Get churro resource information",
		Args:  cobra.MinimumNArgs(1),
		Long:  "This is a command that gets churro resources",
	}

	return cmd
}

func createPipelineCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				logger.Error("pipeline name argument is missing")
				os.Exit(1)
			}
			logger.Infof("pipelineName %s\n", args[0])
			pipelineName := args[0]

			p := v1alpha1.Pipeline{}
			p.ObjectMeta.Name = pipelineName

			err := pipeline.CreatePipeline(logger, p)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}

		},
		Use:   `pipeline`,
		Short: "Command create pipeline",
		Long:  "This is a command that creates a churro pipeline",
	}

	return cmd
}

func deletePipelineCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {

			if len(args) == 0 {
				logger.Error("pipeline argument required")
				os.Exit(1)
			}

			// create the namespace
			/**
			_, config, err := internal.GetKubeClient()
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}
			*/

			// delete a pipeline CR
			pipelineName := args[0]
			err := pipeline.DeletePipeline(logger, pipelineName)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}

			/**
			pipelineClient, err := internal.NewClient(config, pipelineName)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}

			err = pipelineClient.Delete(pipelineName, &metav1.DeleteOptions{})
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}
			*/
			fmt.Printf("delete CR %s\n", pipelineName)

		},
		Use:   `pipeline`,
		Short: "Delete a churro pipeline",
		Long:  "This is a command that deletes a churro pipeline",
	}

	return cmd
}

func getCR(pipelineName string) v1alpha1.Pipeline {
	err := os.Setenv("CHURRO_PIPELINE", pipelineName)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	err = os.Setenv("CHURRO_NAMESPACE", pipelineName)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	pipeline, err := internal.GetPipeline(logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	return pipeline
}

func createCR(client *rest.RESTClient, cr *v1alpha1.Pipeline, ns string) error {
	result := v1alpha1.Pipeline{}

	err := client.Post().
		Resource("pipelines").
		Namespace(ns).
		Body(cr).
		Do(context.TODO()).Into(&result)
	if err != nil {
		logger.Error("error creating cr " + err.Error())
		return err
	}

	logger.Debugf("created cr %s\n", result.ObjectMeta.Labels["name"])
	return err

}

func fillCRDefaults(cr *v1alpha1.Pipeline, pipelineName string) {

	cr.APIVersion = "churro.project.io/v1alpha1"
	cr.Kind = "Pipeline"
	cr.ObjectMeta.Name = pipelineName
	cr.Labels = make(map[string]string)
	cr.Labels["name"] = pipelineName
	cr.Status.Active = "true"
	cr.Status.Standby = []string{"one", "two"}
	cr.Spec.AdminDataSource.Name = "churrodatastore"
	cr.Spec.AdminDataSource.Host = "cockroachdb-public"
	cr.Spec.AdminDataSource.Path = ""
	cr.Spec.AdminDataSource.Port = 26257
	cr.Spec.AdminDataSource.Scheme = ""
	cr.Spec.AdminDataSource.Username = "root"
	cr.Spec.AdminDataSource.Database = "churro"
	cr.Spec.DataSource.Name = "pipelinedatastore"
	cr.Spec.DataSource.Host = "cockroachdb-public"
	cr.Spec.DataSource.Path = ""
	cr.Spec.DataSource.Port = 26257
	cr.Spec.DataSource.Scheme = ""
	cr.Spec.DataSource.Username = pipelineName
	cr.Spec.DataSource.Database = pipelineName
	cr.Spec.LoaderConfig.Location.Scheme = "http"
	cr.Spec.LoaderConfig.Location.Host = "churro-loader"
	cr.Spec.LoaderConfig.Location.Port = 8083
	cr.Spec.LoaderConfig.QueueSize = 30
	cr.Spec.LoaderConfig.PctHeadRoom = 50
	cr.Spec.LoaderConfig.DataSource = v1alpha1.Source{}
	cr.Spec.WatchConfig.Location.Scheme = "http"
	cr.Spec.WatchConfig.Location.Host = "churro-watch"
	cr.Spec.WatchConfig.Location.Port = 8087
}

func createWatchDirCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.CreateWatchDir(logger, cmd, args, serviceCrt, watchDirFile, urlFlag)
		},
		Use:   `watchdir`,
		Short: "Command create watchdir",
		Long:  "This is a command that creates a churro watchdir",
	}

	return cmd
}

func deleteWatchDirCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.DeleteWatchDir(logger, cmd, args, serviceCrt, watchDirId, urlFlag)
		},
		Use:   `watchdir`,
		Short: "Command delete watchdir",
		Long:  "This is a command that deletes a churro watchdir by id",
	}

	return cmd
}

func getWatchDirCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.GetWatchDir(logger, cmd, args, serviceCrt, urlFlag)
		},
		Use:   `watchdir`,
		Short: "Command get watchdir",
		Long:  "This is a command that gets a watchdirs for a given pipeline",
	}

	return cmd
}

func createTransformFunctionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.CreateTransformFunction(logger, cmd, args, serviceCrt, transformFunctionFile, urlFlag)
		},
		Use:   `transformfunction`,
		Short: "Command create transformfunction",
		Long:  "This is a command that creates a transformfunction",
	}

	return cmd
}

func deleteTransformFunctionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.DeleteTransformFunction(logger, cmd, args, serviceCrt, transformFunctionId, urlFlag)
		},
		Use:   `transformfunction`,
		Short: "Command delete transformfunction",
		Long:  "This is a command that deletes a transformfunction by id",
	}

	return cmd
}

func getTransformFunctionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.GetTransformFunction(logger, cmd, args, serviceCrt, urlFlag)
		},
		Use:   `transformfunction`,
		Short: "Command get transformfunction",
		Long:  "This is a command that gets a transformfunction for a given pipeline",
	}

	return cmd
}

func createTransformRuleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.CreateTransformRule(logger, cmd, args, serviceCrt, transformRuleFile, urlFlag)
		},
		Use:   `transformrule`,
		Short: "Command create transformrule",
		Long:  "This is a command that creates a transformrule",
	}

	return cmd
}

func deleteTransformRuleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.DeleteTransformRule(logger, cmd, args, serviceCrt, transformRuleId, urlFlag)
		},
		Use:   `transformrule`,
		Short: "Command delete transformrule",
		Long:  "This is a command that deletes a transformrule by id",
	}

	return cmd
}

func getTransformRuleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			impl.GetTransformRule(logger, cmd, args, serviceCrt, urlFlag)
		},
		Use:   `transformrule`,
		Short: "Command get transformrule",
		Long:  "This is a command that gets a transformrule for a given pipeline",
	}

	return cmd
}
