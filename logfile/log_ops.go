package logfile

import (
	"fmt"
	"sync"

	"6.824/pb"
)

type Log interface {
	Size() int                                             // returns the number of entries in the logfile
	CommitOperation(int, int, *pb.LogElement) (int, error) // commits the operation
	ApplyOperation() (*pb.LogElement, error)               // applies the last committed operation to logfile
	GetFinalTransaction() (*pb.LogElement, error)
	GetTransactionWithIndex(int) (*pb.LogElement, error)
	RemoveEntries(int) (int, error)
}

type Logfile struct {
	logfileLength int
	readyTxn      *pb.LogElement // the last commmitted transaction
	logs          []pb.LogElement
	mu            sync.Mutex
}

func NewLogfile() *Logfile {
	return &Logfile{
		logs: make([]pb.LogElement, 0),
	}
}

// `CommitOperation` is the first step of the two phase commit.
// It is initiated by the `leader` to check whether the requested
// transaction is okay to be committed in the replica
// returns the finalIndex after CommitOperation
func (l *Logfile) CommitOperation(expectedFinalIndex int, currentFinalIndex int, txn *pb.LogElement) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if currentFinalIndex != expectedFinalIndex {
		return currentFinalIndex, fmt.Errorf("final index (%d) not matching expected final index (%d)", currentFinalIndex, expectedFinalIndex)
	}

	// if final index is matching, then add the replica is
	// ready to apply the incoming transaction to the Logfile
	// So, the replica keeps track of this transaction until the
	// second phase of the two phase commit (apply phase)
	l.readyTxn = txn
	l.logs = append(l.logs, *txn)
	l.logfileLength++
	return currentFinalIndex, nil
}

// `ApplyOperation` is the first step of the two phase commit.
// It is initiated by the `leader` to finally apply the previously
// verified transaction in the `commitOperation` step
// `ApplyOperation` applies the committed operation to the logfile.
func (l *Logfile) ApplyOperation() (*pb.LogElement, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.readyTxn == nil {
		return nil, fmt.Errorf("no transaction ready to apply")
	}

	// Apply the transaction to Logfile
	// In this case, the transaction is already added to logs during CommitOperation
	// so no further action is needed other than returning the transaction.
	appliedTxn := l.readyTxn
	l.readyTxn = nil
	return appliedTxn, nil
}

func (l *Logfile) GetFinalTransaction() (*pb.LogElement, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.logs) == 0 {
		return nil, nil
	}
	return &l.logs[len(l.logs)-1], nil
}

func (l *Logfile) GetTransactionWithIndex(index int) (*pb.LogElement, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= len(l.logs) {
		return nil, fmt.Errorf("index out of range")
	}
	return &l.logs[index], nil
}

func (l *Logfile) RemoveEntries(index int) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < 0 || index >= len(l.logs) {
		return -1, fmt.Errorf("index out of range")
	}

	// Remove entries from logs up to the given index
	l.logs = l.logs[:index]
	l.logfileLength = len(l.logs)
	return l.logfileLength, nil
}

func (l *Logfile) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logfileLength
}

// Helper function to stringify data (for logging purposes)
// func stringifyData(data *LogElement) string {
// 	return fmt.Sprintf("%d;%v;%d\n", data.Index, data.Command, data.Term)
// }
