package ctl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateExtractRule(ctx context.Context, request *pb.CreateExtractRuleRequest) (response *pb.CreateExtractRuleResponse, err error) {

	response = &pb.CreateExtractRuleResponse{}
	var rule watch.ExtractRule

	err = json.Unmarshal([]byte(request.ExtractRuleString), &rule)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}

	if rule.WatchDirectoryId == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"extract rule watch directory ID is required")

	}
	if rule.ColumnName == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"extract rule column name is required")
	}
	if rule.RuleSource == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"extract rule source is required")
	}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	err = rule.Create(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	response.Id = rule.Id
	return response, nil
}

func (s *Server) DeleteExtractRule(ctx context.Context, request *pb.DeleteExtractRuleRequest) (response *pb.DeleteExtractRuleResponse, err error) {

	response = &pb.DeleteExtractRuleResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	r := watch.ExtractRule{}
	r.Id = request.ExtractRuleId
	err = r.Delete(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	return response, nil
}

func (s *Server) UpdateExtractRule(ctx context.Context, request *pb.UpdateExtractRuleRequest) (response *pb.UpdateExtractRuleResponse, err error) {

	response = &pb.UpdateExtractRuleResponse{}

	var rule watch.ExtractRule

	err = json.Unmarshal([]byte(request.ExtractRuleString), &rule)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}

	//WatchDirectories[request.PipelineId][request.WatchdirId].ExtractRules[rule.Id] = rule
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	r := watch.ExtractRule{}
	r.Id = rule.Id
	err = rule.Update(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()
	return response, nil
}

func (s *Server) GetExtractRule(ctx context.Context, request *pb.GetExtractRuleRequest) (response *pb.GetExtractRuleResponse, err error) {

	response = &pb.GetExtractRuleResponse{}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	var rule watch.ExtractRule
	rule, err = watch.GetExtractRule(request.ExtractRuleId, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	b, err := json.Marshal(rule)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	response.ExtractRuleString = string(b)

	return response, nil
}

func (s *Server) GetExtractRules(ctx context.Context, request *pb.GetExtractRulesRequest) (response *pb.GetExtractRulesResponse, err error) {

	response = &pb.GetExtractRulesResponse{}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	var rules []watch.ExtractRule
	rules, err = watch.GetExtractRulesForWatchDir(request.WatchdirId, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	fmt.Printf("returning values len %d\n", len(rules))

	b, err := json.Marshal(rules)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}
	response.ExtractRulesString = string(b)

	return response, nil
}
