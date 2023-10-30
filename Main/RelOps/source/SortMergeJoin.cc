
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
                MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
		vector <string> projectionsIn,
		pair <string, string> equalityCheckIn, string leftSelectionPredicateIn,
		string rightSelectionPredicateIn){

	output = outputIn;
	leftTable = leftInputIn;
	rightTable = rightInputIn;
	finalSelectionPredicate = finalSelectionPredicateIn;
	projections = projectionsIn;
	equalityCheck = equalityCheckIn;
	leftSelectionPredicate = leftSelectionPredicateIn;
	rightSelectionPredicate = rightSelectionPredicateIn;
}

void SortMergeJoin :: run () {
	// get the left input record 
	MyDB_RecordPtr leftInputRec = leftTable->getEmptyRecord ();
	MyDB_RecordPtr leftInputRecNext = leftTable->getEmptyRecord ();

    MyDB_RecordPtr rightInputRec = rightTable->getEmptyRecord ();
	MyDB_RecordPtr rightInputRecNext = rightTable->getEmptyRecord ();

	MyDB_BufferManagerPtr parent = leftTable->getBufferMgr();

	int runSize = leftTable->getBufferMgr()->numPages / 2;

	// get the sorted runs
	function <bool ()> leftComp = buildRecordComparator (leftInputRec, leftInputRecNext, equalityCheck.first);
	// Use to check equal for the current left rec and the next left rec
	function <bool ()> leftCompReverse = buildRecordComparator (leftInputRecNext, leftInputRec, equalityCheck.first);
	function <bool ()> rightComp = buildRecordComparator (rightInputRec, rightInputRecNext, equalityCheck.second);

	MyDB_RecordIteratorAltPtr leftIter = buildItertorOverSortedRuns (runSize, *leftTable, leftComp, leftInputRec, leftInputRecNext, leftSelectionPredicate);
	MyDB_RecordIteratorAltPtr rightIter = buildItertorOverSortedRuns (runSize, *rightTable, rightComp, rightInputRec, rightInputRecNext, rightSelectionPredicate);

	// and get the schema that results from combining the left and right records
	MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
	for (auto &p : leftTable->getTable ()->getSchema ()->getAtts ())
		mySchemaOut->appendAtt (p);
	for (auto &p : rightTable->getTable ()->getSchema ()->getAtts ())
		mySchemaOut->appendAtt (p);

	// get the combined record
	MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
	combinedRec->buildFrom (leftInputRec, rightInputRec);

	// now, get the final predicate over it
	func finalPredicate = combinedRec->compileComputation (finalSelectionPredicate);

	// and get the final set of computatoins that will be used to buld the output record
	vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (combinedRec->compileComputation (s));
	}
	
	// compares the left and right input recs
    func equal = combinedRec->compileComputation(" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");
	func less = combinedRec->compileComputation (" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
	func greater = combinedRec->compileComputation (" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");

	// this is the output record
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();
	
	MyDB_PageReaderWriter equalPage (true, *parent);
	vector <MyDB_PageReaderWriter> equalPages;
	
	if (!leftIter->advance () || !rightIter->advance ()) {
		return;
	}

	while (true) {
		bool iterEmpty = false;
		leftIter->getCurrent (leftInputRec);
		rightIter->getCurrent (rightInputRec);

		if (less()->toBool()) {
			if (!leftIter->advance ()) {
				return;
			}
		} else if (greater()->toBool()) {
			if (!rightIter->advance ()) {
				return;
			}
		} else if (equal()->toBool()) {
			equalPage.clear ();
			equalPages.clear ();
			equalPages.push_back (equalPage);
			equalPage.append (leftInputRec);
			
			// find the left rec that have the same val
			while (true) {
				if (!leftIter->advance ()) {
					iterEmpty = true;
					break;
				}
				leftIter->getCurrent(leftInputRecNext);
				
				if (!leftComp() && !leftCompReverse()) {
					if (!equalPage.append(leftInputRecNext)) {
						MyDB_PageReaderWriter newPage (true, *parent);
						equalPage = newPage;
						equalPages.push_back (equalPage);
						equalPage.append (leftInputRecNext);
					}
				} else {
					break;
				}
			}
			
			// Output the match
			while (equal()->toBool()) {
				MyDB_RecordIteratorAltPtr equalIter = getIteratorAlt (equalPages);
				
				while(equalIter->advance()) {
					equalIter->getCurrent(leftInputRec);
					if (finalPredicate ()->toBool ()) {
						// run all of the computations
						int i = 0;
						for (auto &f : finalComputations) {
							outputRec->getAtt (i++)->set (f());
						}

						// the record's content has changed because it 
						// is now a composite of two records whose content
						// has changed via a read... we have to tell it this,
						// or else the record's internal buffer may cause it
						// to write old values
						outputRec->recordContentHasChanged ();
						output->append (outputRec);	
					}
				}
				
				if (!rightIter->advance ()) {
					return;
				}

				rightIter->getCurrent (rightInputRec);
			}			

			if (iterEmpty) {
				return;
			}
		}
	}
}

#endif
