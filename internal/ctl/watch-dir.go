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

func (s *Server) CreateWatchDirectory(ctx context.Context, request *pb.CreateWatchDirectoryRequest) (response *pb.CreateWatchDirectoryResponse, err error) {

	response = &pb.CreateWatchDirectoryResponse{}

	var wdir watch.WatchDirectory

	err = json.Unmarshal([]byte(request.WatchdirString), &wdir)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}

	if wdir.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"watch directory name is required")

	}
	if wdir.Path == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"watch directory path is required")
	}
	if wdir.Scheme == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"watch directory scheme is required")
	}
	if wdir.Regex == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"watch directory regex is required")
	}
	if wdir.Tablename == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"watch directory tablename is required")
	}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	err = wdir.Create(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	fmt.Printf("create watchdir id=%s for ns=%s\n", wdir.Id, request.Namespace)

	response.Id = wdir.Id
	return response, nil
}

func (s *Server) DeleteWatchDirectory(ctx context.Context, request *pb.DeleteWatchDirectoryRequest) (response *pb.DeleteWatchDirectoryResponse, err error) {

	response = &pb.DeleteWatchDirectoryResponse{}
	//delete(WatchDirectories[request.PipelineId], request.WatchdirId)
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	wdir := watch.WatchDirectory{}
	wdir.Id = request.WatchdirId
	err = wdir.Delete(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	return response, nil
}

func (s *Server) GetWatchDirectory(ctx context.Context, request *pb.GetWatchDirectoryRequest) (response *pb.GetWatchDirectoryResponse, err error) {

	response = &pb.GetWatchDirectoryResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	wdir, err := watch.GetWatchDirectory(request.WatchdirId, db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	db.Close()

	b, err := json.Marshal(wdir)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}
	response.WatchdirString = string(b)

	return response, nil
}

func (s *Server) GetWatchDirectories(ctx context.Context, request *pb.GetWatchDirectoriesRequest) (response *pb.GetWatchDirectoriesResponse, err error) {

	response = &pb.GetWatchDirectoriesResponse{}
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	values, err := watch.GetWatchDirectories(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	fmt.Printf("returning values len %d\n", len(values))

	b, err := json.Marshal(values)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}
	response.WatchdirsString = string(b)

	return response, nil
}

func (s *Server) UpdateWatchDirectory(ctx context.Context, request *pb.UpdateWatchDirectoryRequest) (response *pb.UpdateWatchDirectoryResponse, err error) {

	response = &pb.UpdateWatchDirectoryResponse{}

	var f watch.WatchDirectory

	err = json.Unmarshal([]byte(request.WatchdirString), &f)
	if err != nil {
		fmt.Println(err.Error())
		return nil, status.Errorf(codes.InvalidArgument,
			err.Error())
	}

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	err = f.Update(db)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	db.Close()

	return response, nil
}
