package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	STATUS_INIT  = 0
	STATUS_DOING = 1
	STATUS_DONE  = 2
)

type MapStatusInfo struct {
	FilePath   string
	Status     int
	UpdateTime time.Time
}

type ReducerStatusInfo struct {
	FilePaths  []string
	Status     int
	UpdateTime time.Time
	OutputFile string
}

type Master struct {
	// Your definitions here.
	nReduce int
	mu      sync.Mutex

	mapInfo     []MapStatusInfo
	reducerInfo []ReducerStatusInfo
}

func (m *Master) getMapTask() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, _ := range m.mapInfo {
		if m.mapInfo[k].Status == STATUS_INIT || (m.mapInfo[k].Status == STATUS_DOING && time.Now().After(m.mapInfo[k].UpdateTime.Add(10*time.Second))) {
			m.mapInfo[k].Status = STATUS_DOING
			m.mapInfo[k].UpdateTime = time.Now()

			return m.mapInfo[k].FilePath
		}
	}

	return ""
}

func (m *Master) dealMapTaskDone(mf string, rfs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	add := false

	for k, _ := range m.mapInfo {
		if m.mapInfo[k].FilePath == mf {
			if m.mapInfo[k].Status == STATUS_DONE {
				return
			}

			m.mapInfo[k].Status = STATUS_DONE
			add = true

			break
		}
	}

	if add {
		for k, v := range rfs {
			m.reducerInfo[k].FilePaths = append(m.reducerInfo[k].FilePaths, v)
		}
	}
}

func (m *Master) getReducerTask() (int, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	//检查map任务是否完成
	for _, v := range m.mapInfo {
		if v.Status != STATUS_DONE {
			return 0, []string{}
		}
	}

	for k, _ := range m.reducerInfo {
		if m.reducerInfo[k].Status == STATUS_INIT || (m.reducerInfo[k].Status == STATUS_DOING && time.Now().After(m.reducerInfo[k].UpdateTime.Add(10*time.Second))) {
			m.reducerInfo[k].Status = STATUS_DOING
			m.reducerInfo[k].UpdateTime = time.Now()

			return k, m.reducerInfo[k].FilePaths
		}
	}

	return 0, []string{}
}

func (m *Master) dealReducerTaskDone(idx int, rf string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.reducerInfo[idx].Status == STATUS_DONE {
		return
	}

	m.reducerInfo[idx].OutputFile = rf
	m.reducerInfo[idx].Status = STATUS_DONE
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetWork(args GetWorkArgs, reply *GetWorkReply) error {
	mf := m.getMapTask()

	reply.ReducerNum = m.nReduce

	if len(mf) == 0 {
		idx, rf := m.getReducerTask()
		if len(rf) == 0 {
			return nil
		} else {
			reply.Type = 1
			reply.Idx = idx
			reply.Files = rf
		}
	} else {
		reply.Type = 0
		reply.Files = []string{mf}
	}

	return nil
}

func (m *Master) DoneWork(args WorkDoneArgs, reply *WorkDoneReply) error {
	if args.Type == 0 {
		m.dealMapTaskDone(args.Mf, args.Files)
	} else {
		m.dealReducerTaskDone(args.Idx, args.Files[0])
	}
	return nil
}

func (m *Master) IsAllDone(args IsAllDoneArgs, reply *IsAllDoneReply) error {
	reply.Done = m.Done()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	finish := true
	for _, v := range m.reducerInfo {
		if v.Status != STATUS_DONE {
			finish = false
			break
		}
	}

	return finish
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce

	//初始化
	for _, v := range files {
		/*
			fa, err := filepath.Abs(v)
			if err != nil {
				return nil
			}
		*/

		m.mapInfo = append(m.mapInfo, MapStatusInfo{
			FilePath: v,
			Status:   STATUS_INIT,
		})
	}

	for i := 0; i < nReduce; i++ {
		m.reducerInfo = append(m.reducerInfo, ReducerStatusInfo{
			Status: STATUS_INIT,
		})
	}

	go m.server()

	return &m
}
