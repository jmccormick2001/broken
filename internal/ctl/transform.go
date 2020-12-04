package ctl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"gitlab.com/churro-group/churro/internal/transform"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateTransformFunction(ctx context.Context, request *pb.CreateTransformFunctionRequest) (response *pb.CreateTransformFunctionResponse, err error) {

	response = &pb.CreateTransformFunctionResponse{}

	byt := []byte(request.FunctionString)
	var p transform.TransformFunction
	err = json.Unmarshal(byt, &p)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}

	if p.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform name is required")
	}
	if p.Source == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform source is required")
	}

	//p.Id = xid.New().String()
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tf := transform.TransformFunction{}
	tf.Name = p.Name
	tf.Source = p.Source
	err = tf.Create(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	/**
	if TransformFunctions == nil {
		TransformFunctions = make(map[string]map[string]transform.TransformFunction)
	}
	if TransformFunctions[request.PipelineId] == nil {
		TransformFunctions[request.PipelineId] = make(map[string]transform.TransformFunction)
	}
	TransformFunctions[request.PipelineId][p.Id] = p
	fmt.Printf("create tfunction %s %s map=%+v\n", request.PipelineId, p.Id, TransformFunctions)
	*/

	response.Id = tf.Id
	return response, nil
}

func (s *Server) UpdateTransformFunction(ctx context.Context, request *pb.UpdateTransformFunctionRequest) (response *pb.UpdateTransformFunctionResponse, err error) {

	response = &pb.UpdateTransformFunctionResponse{}

	byt := []byte(request.FunctionString)
	var o transform.TransformFunction
	err = json.Unmarshal(byt, &o)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tf, err := transform.GetTransformFunction(o.Id, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tf.Name = o.Name
	tf.Source = o.Source
	err = tf.Update(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	/**
	var ok bool
	var f transform.TransformFunction
	f, ok = TransformFunctions[request.Namespace][o.Id]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform function not found")
	}

	f.Name = o.Name
	f.Source = o.Source

	// do the update by doing a delete then a create

	delete(TransformFunctions[request.PipelineId], f.Id)

	TransformFunctions[request.PipelineId][f.Id] = f
	*/

	return response, nil
}

func (s *Server) GetTransformFunctions(ctx context.Context, request *pb.GetTransformFunctionsRequest) (response *pb.GetTransformFunctionsResponse, err error) {

	response = &pb.GetTransformFunctionsResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	functions, err := transform.GetTransformFunctions(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	b, _ := json.Marshal(functions)
	response.FunctionsString = string(b)

	return response, nil
}

func (s *Server) CreateTransformRule(ctx context.Context, request *pb.CreateTransformRuleRequest) (response *pb.CreateTransformRuleResponse, err error) {

	response = &pb.CreateTransformRuleResponse{}

	byt := []byte(request.RuleString)
	var p transform.TransformRule
	err = json.Unmarshal(byt, &p)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())

	}

	if p.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform rule name is required")
	}
	if p.Path == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform rule path is required")
	}
	if p.Scheme == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform rule scheme is required")
	}
	if p.TransformFunctionName == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"transform function name is required")
	}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	tr := transform.TransformRule{}
	tr.Name = p.Name
	tr.Path = p.Path
	tr.Scheme = p.Scheme
	tr.TransformFunctionName = p.TransformFunctionName
	err = tr.Create(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	/**
	p.Id = xid.New().String()

	if TransformRules == nil {
		TransformRules = make(map[string]map[string]transform.TransformRule)
	}
	if TransformRules[request.PipelineId] == nil {
		TransformRules[request.PipelineId] = make(map[string]transform.TransformRule)
	}
	fmt.Printf("adding transform rule with p=%s id=%s\n", request.PipelineId, p.Id)
	TransformRules[request.PipelineId][p.Id] = p
	fmt.Printf("created TransformRules map %+v\n", TransformRules)
	*/

	response.Id = tr.Id
	return response, nil
}

func (s *Server) DeleteTransformFunction(ctx context.Context, request *pb.DeleteTransformFunctionRequest) (response *pb.DeleteTransformFunctionResponse, err error) {
	response = &pb.DeleteTransformFunctionResponse{}

	fmt.Printf("deleting transform function ns=%s id=%s\n", request.Namespace, request.FunctionId)
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	tf := transform.TransformFunction{}
	tf.Id = request.FunctionId
	err = tf.Delete(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	return response, nil
}

func (s *Server) GetTransformFunction(ctx context.Context, request *pb.GetTransformFunctionRequest) (response *pb.GetTransformFunctionResponse, err error) {

	response = &pb.GetTransformFunctionResponse{}

	/**
	var ok bool
	var f transform.TransformFunction
	f, ok = TransformFunctions[request.PipelineId][request.FunctionId]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument,
			"function not found")

	}
	*/
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tf, err := transform.GetTransformFunction(request.FunctionId, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	db.Close()

	b, _ := json.Marshal(tf)
	response.FunctionString = string(b)

	return response, nil
}

func (s *Server) UpdateTransformRule(ctx context.Context, request *pb.UpdateTransformRuleRequest) (response *pb.UpdateTransformRuleResponse, err error) {

	response = &pb.UpdateTransformRuleResponse{}

	byt := []byte(request.RuleString)
	var o transform.TransformRule
	err = json.Unmarshal(byt, &o)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tr, err := transform.GetTransformRule(o.Id, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tr.Name = o.Name
	tr.Path = o.Path
	tr.Scheme = o.Scheme
	tr.TransformFunctionName = o.TransformFunctionName
	err = tr.Update(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	fmt.Printf("updating transform rule p=%s ruleid=%s\n", request.Namespace, o.Id)
	return response, nil
}

func (s *Server) DeleteTransformRule(ctx context.Context, request *pb.DeleteTransformRuleRequest) (response *pb.DeleteTransformRuleResponse, err error) {

	response = &pb.DeleteTransformRuleResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tr := transform.TransformRule{}
	tr.Id = request.RuleId
	err = tr.Delete(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	return response, nil
}

func (s *Server) GetTransformRules(ctx context.Context, request *pb.GetTransformRulesRequest) (response *pb.GetTransformRulesResponse, err error) {

	response = &pb.GetTransformRulesResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	rules, err := transform.GetTransformRules(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	b, _ := json.Marshal(rules)
	response.RulesString = string(b)

	return response, nil
}

func (s *Server) GetTransformRule(ctx context.Context, request *pb.GetTransformRuleRequest) (response *pb.GetTransformRuleResponse, err error) {

	response = &pb.GetTransformRuleResponse{}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tr, err := transform.GetTransformRule(request.RuleId, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	db.Close()

	b, _ := json.Marshal(tr)
	response.RuleString = string(b)

	return response, nil
}
