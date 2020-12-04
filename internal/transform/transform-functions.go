package transform

import (
	"fmt"
	"strconv"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
	"go.uber.org/zap"
)

func RunRules(scheme string, cols []string, record []string, rules []TransformRule, functions []TransformFunction, logger *zap.SugaredLogger) error {
	// paths can either be an index (int) or a column name (string)
	for _, rule := range rules {
		if rule.Scheme == scheme {
			recordIndex, err := strconv.Atoi(rule.Path)
			if err != nil {
				// assume an error means we have a string
				recordIndex, err = findColumn(cols, rule.Path)
				if err != nil {
					return err
				}
			}
			var fn TransformFunction
			fn, err = findFunction(functions, rule.Name)
			if err != nil {
				return err
			}
			i := interp.New(interp.Options{})
			i.Use(stdlib.Symbols)
			logger.Info("function source is " + fn.Source)
			_, err = i.Eval(fn.Source)
			if err != nil {
				return fmt.Errorf("error in interpreting source %v", err)
			}
			logger.Info("function is " + rule.TransformFunctionName)
			v, err := i.Eval(rule.TransformFunctionName)
			if err != nil {
				return fmt.Errorf("error in interpreting function %v", err)
			}
			record[recordIndex] = v.Interface().(func(string) string)(record[recordIndex])
		}
	}
	return nil
}

func findColumn(cols []string, path string) (int, error) {
	for i := 0; i < len(cols); i++ {
		if cols[i] == path {
			return i, nil
		}
	}
	return 0, fmt.Errorf("could not find column that matches path %s", path)
}

func findFunction(functions []TransformFunction, functionName string) (TransformFunction, error) {
	for i := 0; i < len(functions); i++ {
		if functions[i].Name == functionName {
			return functions[i], nil
		}
	}
	return TransformFunction{}, fmt.Errorf("could not find function %s", functionName)
}
