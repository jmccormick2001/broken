package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type WatchSocket struct {
	Name      string   `json:"name"`
	Path      string   `json:"path"`
	Scheme    string   `json:"scheme"`
	Stocks    []string `json:"stocks"`
	Tablename string   `json:"tablename"`
}

type Endpoint struct {
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Scheme string `json:"scheme"`
}

type Source struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Path      string `json:"path"`
	Port      int    `json:"port"`
	Scheme    string `json:"scheme"`
	Username  string `json:"username"`
	Database  string `json:"database"`
	Tablename string `json:"tablename"`
}

type DBCreds struct {
	CAKey         string `json:"cakey"`
	CACrt         string `json:"cacrt"`
	NodeKey       string `json:"nodekey"`
	NodeCrt       string `json:"nodecrt"`
	ClientRootCrt string `json:"clientrootcrt"`
	ClientRootKey string `json:"clientrootkey"`
	PipelineCrt   string `json:"pipelinecrt"`
	PipelineKey   string `json:"pipelinekey"`
}
type ServiceCreds struct {
	ServiceCrt string `json:"servicecrt"`
	ServiceKey string `json:"servicekey"`
}

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
	Id   string `json:"id"`
	Port int    `json:"port"`

	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Quantity of instances
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	//Size int32 `json:"size,omitempty"`

	// Name of the ConfigMap for GuestbookSpec's configuration
	// +kubebuilder:validation:MaxLength=15
	// +kubebuilder:validation:MinLength=1
	//ConfigMapName string `json:"configMapName"`

	// +kubebuilder:validation:Enum=Phone;Address;Name
	Type            string `json:"alias,omitempty"`
	AdminDataSource Source `json:"adminDataSource,omitempty"`
	DataSource      Source `json:"dataSource,omitempty"`

	WatchSockets []WatchSocket `json:"watchSockets"`
	//WatchDirectories []WatchDirectory `json:"watchDirectories"`
	WatchConfig struct {
		Location Endpoint `json:"location"`
	} `json:"watchConfig"`
	LoaderConfig struct {
		Location    Endpoint `json:"location"`
		QueueSize   int      `json:"queueSize"`
		PctHeadRoom int      `json:"pctHeadRoom"`
		DataSource  Source   `json:"dataSource"`
	} `json:"loaderConfig"`
	DatabaseCredentials DBCreds      `json:"dbcreds,omitempty"`
	ServiceCredentials  ServiceCreds `json:"servicecreds,omitempty"`
}

// PipelineStatus defines the observed state of Pipeline
type PipelineStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// PodName of the active Guestbook node.
	Active string `json:"active"`

	// PodNames of the standby Guestbook nodes.
	Standby []string `json:"standby"`
}

// +kubebuilder:object:root=true

// Pipeline is the Schema for the pipelines API
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec,omitempty"`
	Status PipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineList contains a list of Pipeline
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pipeline{}, &PipelineList{})
}
