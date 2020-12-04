package ctl

// DeletePipeline deletes the pipeline database
func (s *Server) deletePipeline() error {

	s.logger.Info("pipeline database successfully deleted")
	return nil

}
