## Chunks on a file-system
Chunk is a ordered collection of records with FIFO access. The main abstraction which describes chunk in general is defined by `chunk.Chunk` interface. This document describes one of the interface implementations - chunk on a file-system.

## Use cases 
Chunk is a journal of records where the new records are always added to the end. Chunks look ideal for storing sequences of events (logs) with keeping their order of appearance. 

Working with logs, most common operations include the following:
* To read all records from the log
* To read N first records from the top
* To read last N records at the tail
* To read N records with offset M from the top or from the bottom

Usually there is only one source of records that should be added to the chunk.

## Data modifications and the data access

### INSERT
Chunk supports only 1 modification operation - INSERT new records into the end of the collection. There is no need to change existing records, so no UPDATES are supported. In typical case there is only one source of records, so a chunk allows only one INSERT executed at a time.

### DELETE
Chunk doesn't support DELETE operation regarding the following considerations. By limiting a maximum chunk size the log data could be written in to several chunks sequentially. The new chunk could be created every time when the current one reaches the maximum size. Chunks, that contain information from the same source, can be placed in an order and represent a journal of records in the correct order from the source. So, such journal of records is a sequence of chunks where the first chunk contains oldest records and the last one contains latest ones. This case DELETE operation, which makes sense for controling the size of the journal only, can be implemented by removing the whole chunk from the journal. If the first chunk will be removed, the journal size will be prunned by the size of the first chunk. What makes the operation DELETE over the bulk of records be implementated simple and fast:

```
  +------------------------------------------------+
  | +---------+    +---------+         +---------+ |
  | | Chunk 1 |    | Chunk 2 | . . .   | Chunk N | |
  | +---^-----+    +---------+         +---------+ |
  |     |                                          |
  +-----+------------------------------------------+    
        |
    Deleting the chunk, reduces the whole size of the collection on the size of the chunk
```
The DELETE operation is not needed on the chunk level, cause it can be implemented by removing the whole chunk of records from the database.

### READ
Reading chunk data seems most common operation, so it must be optimized. Analyzing the typical use-cases we come up to the following list of operations that make sense to support the read functionality:
* OP_SETP: The operation sets position to n-th record in the collection. n could be in the range [0..N-1], where N is the number of recrods in the chunk. 
* OP_READ: Read operation allows to read record from the current position
* OP_GETP: the operation returns the current record position
* OP_NEXT: To shift current position to the next record

Creating a data structure sutable for implementing this 4 operations, will allow to make the READ functionality highly performant. 

## Chunk Id
A chunk Id is 64bit unsigned integer which consists of 2 parts - first part is UNIX timestamp in nanoseconds truncated to 48bits, and the second part - 16bits value contains the process random. 
```
    +---------------+------------------------------------------+
    | random value  | UNIX time in nanoseconds (highest 48bit) |
    +---------------+------------------------------------------+
     0            15 16                                      63
```
When chunk Id is encoded as string it is represented as 16-base number with 16 digits length padded by 0s if needed. For instance, Id=127 (`0x7F`) will be encoded as `000000000000007F`.

Because of creating of new chunk is a `rear` operation (several chunks per second can be created in a very aggressive environment), the chunk Ids could be used for uniquely identifying chunks and for sorting them by the time of creation as well. 

## Chunks structure
It is proposed to keep chunk's records in a file where new records are simply appended to the end of the file. For implementing OP_READ we need to know the offset of current record from the top of the file. For implementing OP_NEXT we need to know offset for the current record and the size of the record to find out position of the next record. 
It would be simple if for every record we would save its size first and then the record itself. With this format of the file structure, the operations OP_READ, OP_NEXT and OP_GETP seem to be implemented effectively, but for implementation of OP_SETP we still need to iterate from the top of the file to the desired record number to apply OP_NEXT number of times. This could be very ineffective for number of records, so we need a way to have OP_SETP be implemented more effectively. 

Building an index file with the fixed records size, would allow us to implement OP_SETP by reading only one record from the index to discover the offset of n-th record in the data file. Doing this we have all this operations implemented effectively. 

So the chunk normally will consist of 2 files:
* the data file
* the index file

Having consistent the data file is quite enough to build index file on it, by just walking through all records and record their offsets into the index file.

## Chunk file naming convention
The chunk data file must have name `<the chunk Id>.dat`, and the index file is `<the chunk Id>.idx`. Using the chunk Id in the files names guarantee that there are no 2 different chunks will have the same file name. 

### The chunk data file structure
A chunk data file is a binary file where every record is represented by its size and the record data itself:
```
    +-------------------------+
    | Record 0 size (4 bytes) | <- record 0, offset=0
    +-------------------------+
    |       Record 0 data     |
    |          N bytes        |
    +-------------------------+
    | Record 1 size (4 bytes) | <- record 1, offset=N+4
    +-------------------------+
    |       Record 1 data     |    
    |           . . .         |
```
### The chunk index file structure
A chunk index file has the following structure
```
    +-------------------------+
    | Record 0 offst (8 bytes) | <- record 0 offset, offset=0
    +-------------------------+
    | Record 1 offst (8 bytes) | <- record 1 offset, offset=8
    +-------------------------+
    |           . . .         |
```
To calculate for n-th record offset using the index file, we need to read 8 bytes by 8*n offset from the index file and receive the offset in the data file. 

## Write (INSERT) records operarion
Write operation is simple, but it has a potential corner case, which should be adressed. In general to insert a new record, we just need to write the size of the record and the record itself into the data file. Updating the index file includes adding 8 bytes of the record offset, which starts from the record size. The write operation should be properly synchronized to have a guarantee that no other writers to do the same things simultaneously. What means only one writer at a time. This rule guarantees that the record size and the record data will not be deliminated by bytes of other writer. 

The corner case that we have to think about relates to read operations while the write is happening. The problem is that the read operation can have access to the partially written data either because of the write operation is not completed, or by because of write operation buffers is not synced with the file-system yet. To address the case we need to be sure that the read is happening for the data which is synced with the file-system. To implement it we will keep the position of confirmed data which was synced to the file-system to guarantee that the read will give conistent results. Write operation uses a buffer, which is synced with file-system every time when the buffer is full or explicitly. When many writes happens we should not care about the buffer sync, because it happens due to the buffer overflown automatically. But when there are no writes anymore we have to invoke sync() by a so-called flush timeout. This guarantee that the data will be synced to the file-system after a write operation and make it be ready for a read operation. Write keeps a pointer to the position of the synced data, so the read operation never tries to read below the position:
```
    +------------+
    | Record 0   | 
    +------------+
    | Record 1   | 
    +------------+
    |    ...     |
    | Record N   |  <-- confirmed (synced) last record
    +------------+
    | Record N+1 | 
    +------------+
    | Record N+2 | 
    +------------+    
                    <-- Next record write position
```
 On the illustration reader will not try to read below the confirmed record, until it is moved below. So Records N+1 and N+2 is not visible for read for the time.
 
## Read operation
Read operation is simple, but as it is explained in `Write` operation, some portion of written records are in a blind zone and can be invisible for a the `Read` operation until they are synced and the position for confirmed last record will not be updated.

## Chunk iterator
Chunk iterator is a tool for reading chunk records in the order, but it extends the standard iterator (`records.Iterator`) functionality by changing the current record position. Having the functionlity (OP_SETP) allows to jump to any record in the chunk and start iteration process out of there effectively. 