
#ifndef REG_SELECTION_C                                        
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection :: RegularSelection (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
		string selectionPredicateIn, vector <string> projectionsIn) {
        
    output = outputIn;
    selectionPredicate = selectionPredicateIn;
    projections = projectionsIn;
    myTable = inputIn;
 }

void RegularSelection :: run () {
    MyDB_RecordPtr inputRec = myTable->getEmptyRecord();

	// now, get the final predicate over it
	func selectPredicate = inputRec->compileComputation (selectionPredicate);

    // and get the final set of computatoins that will be used to buld the output record
	vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (inputRec->compileComputation (s));
	}

	// this is the output record
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();

    MyDB_RecordIteratorAltPtr myIter = myTable->getIteratorAlt();

    while(myIter->advance()) {
        myIter->getCurrent(inputRec);

        if (selectPredicate()->toBool()) {
            // run all of the computations
            int i = 0;
            for (auto &f : finalComputations) {
                outputRec->getAtt (i++)->set (f());
            }
            outputRec->recordContentHasChanged ();
            output->append (outputRec);	
        }
    }
}

#endif
