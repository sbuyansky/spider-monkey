/*
Team Skrentny
Connor Zarecki		<zarecki>	9063430798
Ryan Nie			<shuai>		9066285421
Spencer Buyansky	<buyansky>	9066247777

This file implements the buffer manager, making all necessary changes to the
data in the buffer pool so that data can be transfered between main memory and
disk.
*/

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
	cerr << "At line " << __LINE__ << ":" << endl << "  "; \
	cerr << "This condition should hold: " #c << endl; \
	exit(1); \
			 } \
				   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
	numBufs = bufs;

	bufTable = new BufDesc[bufs];
	memset(bufTable, 0, bufs * sizeof(BufDesc));
	for (int i = 0; i < bufs; i++) 
	{
		bufTable[i].frameNo = i;
		bufTable[i].valid = false;
	}

	bufPool = new Page[bufs];
	memset(bufPool, 0, bufs * sizeof(Page));

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

	// flush out all unwritten pages
	for (int i = 0; i < numBufs; i++) 
	{
		BufDesc* tmpbuf = &bufTable[i];
		if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
			cout << "flushing page " << tmpbuf->pageNo
				<< " from frame " << i << endl;
#endif

			tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
		}
	}

	delete [] bufTable;
	delete [] bufPool;
}

/*
Allocates a free frame using the clock algorithm; if necessary, writing a dirty
page back to disk. Returns BUFFEREXCEEDED if all buffer frames are pinned,
UNIXERR if the call to the I/O layer returned an error when a dirty page was
being written to disk and OK otherwise.  This private method will get called by
the readPage() and allocPage() methods described below.
If the buffer frame allocated has a valid page in it, it will remove the
appropriate entry from the hash table.
frame has the index of the frame number freed.
*/
const Status BufMgr::allocBuf(int & frame) 
{
#ifdef DEBUG
	cout << "allocBuf" << endl;
#endif
	advanceClock();

	int pinnedPagesCount = 0;

	BufDesc * curDesc = NULL;

	//loop around the clock until you reach
	//the original position of the clockhand
	while(pinnedPagesCount < this->numBufs){
		curDesc = &bufTable[clockHand];
		//if not valid return that frame
		if(!curDesc->valid){
			frame = clockHand;
			curDesc->Clear();
			return OK;
		}
		//if valid
		else{
			//if refbit is set, unset it
			if(curDesc->refbit){
				curDesc->refbit = false;
			}
			//if refbit is unset
			else{
				//not pinned
				if(curDesc->pinCnt == 0){
					if(curDesc->dirty){
						//flush page to disk
						if(curDesc->file->writePage(curDesc->pageNo, &(bufPool[clockHand]))!= OK){
							return UNIXERR;
						}
					}
					//remove from hashtable
					hashTable->remove(curDesc->file, curDesc->pageNo);

					//use the page
					curDesc->Clear();
					frame = clockHand;
					return OK;
				}
				else{
					pinnedPagesCount++;
				}
			}
		}
		advanceClock();
	}

	//got through the entire clock and
	//couldn't find a page to return
	return BUFFEREXCEEDED;
}

/*
Tries to read a specific page of the file. If the file is in the hashtable
already, it just sets the reference bit and increments the pin count. If it
isn't in the table, it gets a new frame and reads the page into memory, finally
setting the page count to 1.
file is the disk file to be read into the hashtable
PageNo the page of the file to read
page the appropriate page in the that was requested
Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table
error occurred.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
#ifdef DEBUG
	cout << "readPage" << endl;
#endif
	int frameNum = -1;
	Status lookSt = hashTable->lookup(file, PageNo, frameNum);
	if(lookSt == HASHNOTFOUND) {
#ifdef DEBUG
		cout << "readPage:page does not exist in hashtable" <<endl;
#endif
		// page not in buffer pool, so allocating a new page
		Status allocSt = allocBuf(frameNum);
		if(allocSt == UNIXERR) {
			return UNIXERR;
		} else if(allocSt == BUFFEREXCEEDED) {
			return BUFFEREXCEEDED;
		} else if(allocSt == OK) {
			// page has been allocated
			// call the method file->readPage() to read the page from disk into the buffer pool frame
			Status readSt = file->readPage(PageNo,&bufPool[frameNum]);
			if(readSt == BADPAGEPTR) {
				//TODO
			} else if(readSt == BADPAGENO) {
				//TODO
			} else if(readSt == UNIXERR) {
				return UNIXERR;
			} else if(readSt == OK) {
				// insert the page into the hashtable
				Status insertSt = hashTable->insert(file, PageNo, frameNum);
				if(insertSt == OK) {
					// invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for the page set to 1.
					bufTable[frameNum].Set(file, PageNo);
					page = &bufPool[frameNum];
					// return a pointer to the frame containing the page via the page parameter
					return OK;

				} else {
					return HASHTBLERROR;
				}
			} else {
				// should not reach here
				cout << "Error occurred: readPage didn't return valid Status";
			}
		} else {
			// should not reach here
			cout << "Error occurred: allocBuf didn't return valid Status";
		}
	} else if(lookSt == OK) {
		// found page in pool
		// set refbit and pinCount
#ifdef DEBUG
		cout << "readPage:page exists in hashtable" <<endl;
#endif
		bufTable[frameNum].refbit = true;
		bufTable[frameNum].pinCnt++;
		page = &(bufPool[frameNum]);
		return OK;

	} else {
		// should not reach here
		cout << "Error occurred: lookup didn't return valid Status";
	}

	return OK;
}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if
dirty == true, sets the dirty bit.
file is the disk file with the page
PageNo is the page to unpin
dirty if true, sets the dirty bit in the table
Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer
pool hash table, PAGENOTPINNED if the pin count is already 0.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
							   const bool dirty) 
{
#ifdef DEBUG
	cout << "unPinPage" << endl;
#endif
	int frameNo = -1;
	if(hashTable->lookup(file, PageNo, frameNo) != HASHNOTFOUND){
		if(bufTable[frameNo].pinCnt == 0){
			return PAGENOTPINNED;
		}
		else{
			bufTable[frameNo].pinCnt--;
		}

		if(dirty == true){
			bufTable[frameNo].dirty = true;
		}
	}
	else{
		return HASHNOTFOUND;
	}
	return OK;

}

/*
Creates a new page in the file for the storage of more data. It first allocates
an empty page in the specified file by invoking file->allocatePage(). This
returns the page number of the newly allocated page. allocBuf() is called to
obtain a buffer pool frame. Next, an entry is inserted into the hash table and
Set() is invoked on the frame to set it up properly.
file the file to create a new page on
pageNO the page number of the newly allocated page
page a pointer to the buffer frame allocated for the page
Returns OK if no errors occurred, UNIXERR if a Unix error occurred,
BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table
error occurred. 
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
#ifdef DEBUG
	cout << "allocPage" << endl;
#endif
	//allocate a page
	if(file->allocatePage(pageNo) != OK){
		cout <<"BufMgr::allocPage, error allocating a page in file" <<endl;
	}
	int frameNo = 0;

	Status s = allocBuf(frameNo);
	if(s == OK){
		//insert into hashtable
		if(hashTable->insert(file, pageNo, frameNo) != OK){
			return HASHTBLERROR;
		}
		//invoke set on the new frame
		bufTable[frameNo].Set(file, pageNo);

		//return pointer to buffer frame allocated for the page
		//do not know if this is correct

		file->readPage(pageNo, &bufPool[frameNo]);

		page =  &bufPool[frameNo];
	}

	return s;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
	// see if it is in the buffer pool
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, pageNo, frameNo);
	if (status == OK)
	{
		// clear the page
		bufTable[frameNo].Clear();
	}
	status = hashTable->remove(file, pageNo);

	// deallocate it in the file
	return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
	Status status;

	for (int i = 0; i < numBufs; i++) {
		BufDesc* tmpbuf = &(bufTable[i]);
		if (tmpbuf->valid == true && tmpbuf->file == file) {

			if (tmpbuf->pinCnt > 0)
				return PAGEPINNED;

			if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
				cout << "flushing page " << tmpbuf->pageNo
					<< " from frame " << i << endl;
#endif
				if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					&(bufPool[i]))) != OK)
					return status;

				tmpbuf->dirty = false;
			}

			hashTable->remove(file,tmpbuf->pageNo);

			tmpbuf->file = NULL;
			tmpbuf->pageNo = -1;
			tmpbuf->valid = false;
		}

		else if (tmpbuf->valid == false && tmpbuf->file == file)
			return BADBUFFER;
	}

	return OK;
}


void BufMgr::printSelf(void) 
{
	BufDesc* tmpbuf;

	cout << endl << "Print buffer...\n";
	for (int i=0; i<numBufs; i++) {
		tmpbuf = &(bufTable[i]);
		cout << i << "\t" << (char*)(&bufPool[i]) 
			<< "\tpinCnt: " << tmpbuf->pinCnt;

		if (tmpbuf->valid == true)
			cout << "\tvalid\n";
		cout << endl;
	};
}


