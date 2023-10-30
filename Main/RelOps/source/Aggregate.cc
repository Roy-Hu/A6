
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

using namespace std;

Aggregate :: Aggregate (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
		vector <pair <MyDB_AggType, string>> aggsToComputeIn,
		vector <string> groupingsIn, string selectionPredicateIn) {

    input = inputIn;
    output = outputIn;
    aggsToCompute = aggsToComputeIn;
    groupings = groupingsIn;
    selectionPredicate = selectionPredicateIn;
}

void Aggregate :: run () {
	MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
	// evaluate the aggregation is to create a schema that can store all of
	// the required aggregate and grouping attributes
	MyDB_RecordPtr inputRec = input->getEmptyRecord ();

	for (auto &p : output->getTable ()->getSchema ()->getAtts ())
		aggSchema->appendAtt (p);
	MyDB_RecordPtr aggRec = make_shared <MyDB_Record> (aggSchema);

	vector <func> groupFunc;
	for (auto &g : groupings) {
		groupFunc.push_back(inputRec->compileComputation(g));
	}

	// now get the predicate
    func pred = inputRec->compileComputation (selectionPredicate);

	// and get the  set of aggrate computatoins that will be used to buld the aggregate record
	vector <pair <MyDB_AggType, func>> aggsComputations;
	for (auto &a : aggsToCompute) {
		aggsComputations.push_back (make_pair(a.first, inputRec->compileComputation (a.second)));
	}

	// get all of the pages
	vector <MyDB_PageReaderWriter> allData;
	for (int i = 0; i < input->getNumPages (); i++) {
		MyDB_PageReaderWriter temp = (*input)[i];
		if (temp.getType () == MyDB_PageType :: RegularPage)
			allData.push_back (temp);
	}
	
	// this is the hash map we'll use to look up groups... the key is the hashed value
	// of all of the attributtes in group, and the value is a list of pointers were all
	// of the records with that hsah value are located
	unordered_map <size_t, void *> myHash;
	unordered_map <size_t, int> cntHash;
	
	MyDB_RecordIteratorAltPtr myIter = getIteratorAlt (allData);

	MyDB_AttValPtr zeroInt = make_shared <MyDB_IntAttVal> ();
	MyDB_AttValPtr zeroDouble = make_shared <MyDB_DoubleAttVal> ();

	int pageCnt = 0;
	while (myIter->advance ()) {
		// hash the current record
		myIter->getCurrent (inputRec);

		if (!pred ()->toBool ()) {
			continue;
		}

		// compute its hash
		size_t hashVal = 0;
		for (auto &g : groupFunc) {
			hashVal ^= g ()->hash ();
		}

		// Creat a group record if hash doesn't found
		if (myHash.count(hashVal) == 0) {
			int i = 0;
			for (auto &g : groupFunc) {
				aggRec->getAtt(i++)->set(g());
			}

			cntHash[hashVal] = 1;
			for (auto &a : aggsComputations) {
				switch (a.first)
				{
				case MyDB_AggType::sum: {
					MyDB_IntAttValPtr att = make_shared <MyDB_IntAttVal> ();
					att->set(a.second()->toInt());
					aggRec->getAtt(i++)->set(att);
					break;
				}
				case MyDB_AggType::cnt: {
					MyDB_IntAttValPtr att = make_shared <MyDB_IntAttVal> ();
					att->set(1);
					aggRec->getAtt(i++)->set(att);
					break;
				}
				case MyDB_AggType::avg: {
					MyDB_DoubleAttValPtr att = make_shared <MyDB_DoubleAttVal> ();
					att->set(a.second()->toDouble());
					aggRec->getAtt(i++)->set(att);
					break;
				}
				default:
					break;
				}

				aggRec->recordContentHasChanged ();
			} 

			// save the address for the group record, if in the future there are
			// records belongs to the same group we can access it
			MyDB_PageReaderWriter tmp = output->getPinned(pageCnt);
			void* loc = tmp.appendAndReturnLocation(aggRec);
			if (loc == nullptr) {
				tmp = output->getPinned(++pageCnt);
				loc = tmp.appendAndReturnLocation(aggRec);
			}

			myHash [hashVal] = loc;
		} else {
			//  get the address of the group record
			aggRec->fromBinary (myHash[hashVal]);
			cntHash[hashVal]++;

			int i = groupFunc.size();
			for (auto &a : aggsComputations) {
				switch (a.first)
				{
				case MyDB_AggType::sum: {
					MyDB_IntAttValPtr att = make_shared <MyDB_IntAttVal> ();
					att->set(aggRec->getAtt(i)->toInt() + a.second()->toInt());
					aggRec->getAtt(i++)->set(att);
					break;
				}
				case MyDB_AggType::cnt: {
					MyDB_IntAttValPtr att = make_shared <MyDB_IntAttVal> ();
					att->set(cntHash[hashVal]);
					aggRec->getAtt(i++)->set(att);
					break;
				}
				case MyDB_AggType::avg:{
					MyDB_DoubleAttValPtr att = make_shared <MyDB_DoubleAttVal> ();
					att->set((aggRec->getAtt(i)->toDouble() * (cntHash[hashVal] - 1)+ a.second()->toDouble()) / cntHash[hashVal]);
					aggRec->getAtt(i++)->set(att);
					break;
				}
				default:
					break;
				}
			} 

			// write back to group record
            aggRec->recordContentHasChanged ();
            aggRec->toBinary(myHash[hashVal]);
		}
	}
}

#endif

