package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetWorkArgs struct {
}

type GetWorkReply struct {
	Type       int //0-map 1-reducer
	Files      []string
	Idx        int
	ReducerNum int
}

type WorkDoneArgs struct {
	Type  int
	Files []string
	Mf    string
	Idx   int
}

type WorkDoneReply struct {
}

type IsAllDoneArgs struct {
}

type IsAllDoneReply struct {
	Done bool
}
