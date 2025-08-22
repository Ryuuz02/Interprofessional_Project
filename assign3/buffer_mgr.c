#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int buffer_Size = 0;
int last_Index = 0;
int count = 0;
int hit_Count = 0;
int clock_Pointer = 0;
int lfu_Pointer = 0;
RC Rcode;

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
	Rcode=RC_OK;
	if(bm==NULL){
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		bm->strategy=strategy;
		bm->pageFile=(char *)pageFileName;
		bm->numPages=numPages;
		if(pageFileName==NULL){
			return RC_FILE_NOT_FOUND;
		}
		else{
		int size = sizeof(Frame);
		int buff=numPages*size;
		Frame *p=malloc(buff);
		buffer_Size=0;
		buffer_Size=numPages;

		for(int i=0;i<buffer_Size;i++){
			p[i]=(Frame){.data=NULL,.pageNum = -1,.dirtySegment = 0,.fixCount = 0,.hitNum = 0,.index = 0};
		}
		lfu_Pointer = 0;	
		bm->mgmtData = p;
		clock_Pointer = 0;
		count = 0;			
		}
	}	
	return Rcode;
}

extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	Rcode=RC_OK;
	if(bm==NULL){
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		Frame *pf = (Frame *)(*bm).mgmtData;
		int as = forceFlushPool(bm);
		if(as==Rcode){
			int i=0;
			do{
				if(!(i>buffer_Size || i==buffer_Size)){
					if(pf[i].fixCount !=0 ){
						return RC_PINNED_PAGES_IN_BUFFER;
					}
				}
				i++;
			}while(!(i>buffer_Size || i==buffer_Size));
			(*bm).mgmtData=NULL;
			free(pf);			
		}			
		else{
			return RC_WRITE_BACK_FAILED;
		}
	}		
	return Rcode;
}

extern RC forceFlushPool(BM_BufferPool *const bm)
{
	Rcode=RC_OK;
	Frame *pf = (Frame *)(*bm).mgmtData;
	if(bm!=NULL){
		int temp=0;
		for(temp=0;temp<buffer_Size;temp++){
			if(temp<buffer_Size){
				if(pf[temp].fixCount==0){
					if(pf[temp].dirtySegment==1){
						SM_FileHandle file_handle;
						if(openPageFile((*bm).pageFile,&file_handle)==RC_OK){							
						}		
						int zro=0;
						if(1)				 
							writeBlock(pf[temp].pageNum,&file_handle,pf[temp].data);
						pf[temp].dirtySegment=zro;
						if(temp<buffer_Size)
							count++;
					}
				}
			}
		}
	}
	else{
		return RC_BUFF_POOL_NOT_FOUND;
	}
	return Rcode;
}

extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const p)
{
	Rcode=RC_OK;
	if(bm==NULL){
		printf("The buffer pool not exists");
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		if(p==NULL){
			printf("Invalid Page");
			return RC_PAGE_ERROR;
		}
		else{
			Frame *pf = (Frame *)(*bm).mgmtData;
			if(1){
				for(int i=0;i<buffer_Size;i++){
					if(!(pf[i].pageNum<(p->pageNum))){
						if(!(pf[i].pageNum>(p->pageNum))){
							pf[i].dirtySegment=1;
							return RC_OK;
						}
					}
					else{
						if((pf[i].pageNum==(p->pageNum))){
							pf[i].dirtySegment=1;
							return RC_OK;
						}
					}
				}
			}
		}
	}
	
	return RC_ERROR;
}

extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const p)
{	
	Frame *pf = (Frame *)(*bm).mgmtData;
	int i=0;
	if(bm==NULL){
		printf("The Buffer poool not exists");
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		if(p==NULL){
			printf("Page is Invalid");
			return RC_PAGE_ERROR;
		}
		else{
			do{
				if(!(i>buffer_Size || i == buffer_Size)){
					if(pf[i].pageNum!=p->pageNum){
						pf[i].fixCount=pf[i].fixCount;
					}
					else{
						pf[i].fixCount=pf[i].fixCount-1;
					}
				}
				i++;
			}while(i<buffer_Size);
		}
	}
	return RC_OK;
}

extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const p)
{
	Rcode=RC_OK;
	if(bm==NULL){
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		int temp=0;	
		int buffer=0;
		Frame *pf = (Frame*)(*bm).mgmtData;	
		do{
			if(!(temp>buffer_Size||temp==buffer_Size)){
				if(!(pf[temp].pageNum < p->pageNum || pf[temp].pageNum > p->pageNum)){
					SM_FileHandle file_handle;
					if(openPageFile((*bm).pageFile, &file_handle) != RC_OK){						
					}
				writeBlock(pf[temp].pageNum, &file_handle, pf[temp].data);				
				pf[temp].dirtySegment = buffer;
				count++;	
				}
			}
			temp++;
		}while(!(temp>buffer_Size||temp==buffer_Size));
	}	
	return Rcode;
}

extern void FIFO(BM_BufferPool *const bm, Frame *p)
{
	int position=0;
	if(bm!=NULL){
		Frame *pf = (Frame *)(*bm).mgmtData;
		position = last_Index % buffer_Size;
		for(int temp=0;temp<buffer_Size;temp++){
			if(pf[position].fixCount == 0){
				if(!(pf[position].dirtySegment !=1 )){
					SM_FileHandle file_handle;
					openPageFile((*bm).pageFile, &file_handle);
					writeBlock(pf[position].pageNum,&file_handle,pf[position].data);
					count=count+1;
				}
				pf[position]=(Frame){
					.data=(*p).data,
					.fixCount=(*p).fixCount,
					.hitNum=(*p).hitNum,
					.pageNum=(*p).pageNum,
					.dirtySegment=(*p).dirtySegment
					};
				break;
			}
			else{
				position=position+1;
				if(position%buffer_Size == 0)
					position=0;
			}
		}
	}
}

extern void LFU(BM_BufferPool *const bm, Frame *p)
{

	Frame *pf=(Frame*)(*bm).mgmtData;
	if(bm==NULL){
		printf("Buffer File Not Found");
	}
	else{
		int index=0,leastFreqRef=0;
		index = lfu_Pointer;	

		int temp=0;
		a:
		if(!(temp>buffer_Size || temp==buffer_Size)){
			if(pf[index].fixCount==0){
				index = (index + temp) % buffer_Size;
				leastFreqRef = pf[index].index;
			}
		}
		temp++;
		if(!(temp>buffer_Size || temp==buffer_Size))
			goto a;

		temp=(index+1);
		temp=temp%buffer_Size;

		int j=0;
		b:
		if(!(j>buffer_Size || j==buffer_Size)){
			if(pf[temp].index < leastFreqRef){
				index=temp;
				leastFreqRef=pf[temp].index;
			}
			temp=temp+1;
			temp=temp%buffer_Size;
		}
		j++;
		if(!(j>buffer_Size || j==buffer_Size))
			goto b;

		if(pf[index].dirtySegment == 1)
		{
			SM_FileHandle file_handle;
			openPageFile((*bm).pageFile, &file_handle);
			writeBlock(pf[index].pageNum,&file_handle,pf[index].data);
			if(1){
			 count++;
			}
		}
		
		pf[index]=(Frame){
			.data=p->data,
			.dirtySegment=p->dirtySegment,
			.pageNum=p->pageNum,			
			.fixCount=p->fixCount
			};
		lfu_Pointer=index+1+0;
	}	
}

extern void LRU(BM_BufferPool *const bm, Frame *p)
{	
	Frame *pf = (Frame *) (*bm).mgmtData;
	int temp=0,lefthead, head;

	bool flag = true;
	a:
	if(!(temp>buffer_Size || temp==buffer_Size)){
		if(!(pf[temp].fixCount!=0)){
			head=pf[temp].hitNum;
			lefthead=temp;
			flag=false;
		}
	}
	temp++;
	if(!(temp>buffer_Size || temp==buffer_Size)&&flag)
		goto a;
		

	temp=1+lefthead;
	b:
	if(!(temp>buffer_Size || temp==buffer_Size)){
		if(pf[temp].hitNum<head){
			head=pf[temp].hitNum;
			lefthead=temp;
		}
	}

	temp++;
	if(!(temp>buffer_Size || temp==buffer_Size))
		goto b;

	if(!(pf[lefthead].dirtySegment!=1))
	{
		count++;
		SM_FileHandle file_handle;
		openPageFile((*bm).pageFile,&file_handle);
		writeBlock(pf[lefthead].pageNum,&file_handle,pf[lefthead].data);		
	}

	pf[lefthead]=(Frame){
		.fixCount=p->fixCount,
		.data=p->data,
		.pageNum=p->pageNum,
		.hitNum=p->hitNum,		
		.dirtySegment=p->dirtySegment
		};
}

extern void CLOCK(BM_BufferPool *const bm, Frame *p)
{	
	Frame *pf = (Frame *)  (*bm).mgmtData;
	if(bm==NULL){
		while(1){
			clock_Pointer = (clock_Pointer%buffer_Size != 0)	? clock_Pointer : 0;
			if(pf[clock_Pointer].hitNum<0||pf[clock_Pointer].hitNum>0){
				pf[clock_Pointer++].hitNum=0;
			}
			else{
				SM_FileHandle file_handle;
				int filestatclk;
				if((pf[clock_Pointer].dirtySegment == 1)){
					filestatclk=openPageFile(bm->pageFile, &file_handle);
					if(filestatclk)
					count=count;
					writeBlock(pf[clock_Pointer].pageNum, &file_handle, pf[clock_Pointer].data);
					count++;
				}

				pf[clock_Pointer]=(Frame){
					.data=(*p).data,
					.fixCount=(*p).fixCount,
					.hitNum=(*p).hitNum,
					.pageNum=(*p).pageNum,
					.dirtySegment=(*p).dirtySegment
					};
				clock_Pointer=clock_Pointer+1;

				break;	
			}
		}
	}
}

extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const p, const PageNumber pageNum)
{
	Rcode=RC_OK;
	
	if(bm==NULL){
		return RC_BUFF_POOL_NOT_FOUND;
	}
	else{
		if(p==NULL){
			return RC_PAGE_ERROR;
		}
		else{
			Frame *pf = (Frame *)(*bm).mgmtData;
			if(pf[0].pageNum!=-1)
			{
				bool flag=true;
				bool isBufferFull = true;
				int temp=0;	

				a:
				if(!(temp>buffer_Size || temp==buffer_Size)){
					if(pf[temp].pageNum==-1){
							int pg=PAGE_SIZE;
							SM_FileHandle file_handle;
							openPageFile((*bm).pageFile,&file_handle);
							pf[temp]=(Frame){
								.data = (SM_PageHandle) malloc(pg)
							};
							pf[temp].pageNum = pageNum;
							int zri=0;
							readBlock(pageNum,&file_handle,pf[temp].data);
							int cnt=1;		
							hit_Count++; 
							pf[temp].index = zri;					
							pf[temp].fixCount = cnt;							
							last_Index++;	
							
							if(RS_LRU==(*bm).strategy){
								pf[temp].hitNum=hit_Count;
							}
							else if(RS_LRU==(*bm).strategy){
								pf[temp].hitNum=1;
							}
							isBufferFull=false;
							p->data=pf[temp].data;
							p->pageNum=pageNum;
							flag = false;
					}
					else{
						if(pf[temp].pageNum==pageNum){
							isBufferFull = false;
							pf[temp].fixCount=pf[temp].fixCount+1;
							hit_Count++; 
							if((*bm).strategy==RS_CLOCK){
								pf[temp].hitNum = 1;
							}
							else if((*bm).strategy==RS_LRU){
								pf[temp].hitNum = hit_Count;
							}
							else if((*bm).strategy==RS_LFU){
								pf[temp].index++;
							}

							flag = false;
							p->data=pf[temp].data;
							p->pageNum=pageNum;
							clock_Pointer=clock_Pointer+1;
						}
					}
					
				}
				temp++;
				if(temp<buffer_Size && flag)
					goto a;


				if(isBufferFull){
					SM_FileHandle file_handle;
					Frame *newPage=(Frame*)malloc(sizeof(Frame));
					openPageFile((*bm).pageFile,&file_handle);
					newPage->data=(SM_PageHandle)malloc(PAGE_SIZE);
					readBlock(pageNum,&file_handle,(*newPage).data);
					last_Index++;
					if(flag){
						newPage->dirtySegment=0;
						newPage->pageNum=pageNum;
						newPage->index=0;
						newPage->fixCount=1;							
					}
					hit_Count++;
					if(RS_CLOCK==(*bm).strategy){
						(*newPage).hitNum=1;
					}						
					else if((*bm).strategy==RS_LRU){
						(*newPage).hitNum=hit_Count;
					}
					p->pageNum=pageNum;
					p->data=newPage->data;
					switch ((*bm).strategy)
					{
					case RS_LRU:LRU(bm, newPage);
						break;
					case RS_FIFO:FIFO(bm, newPage);
						break;
					case RS_CLOCK:CLOCK(bm, newPage);
						break;
					case RS_LFU:LFU(bm, newPage);
						break;
					default:
						break;
					}	
				}						
				
				return Rcode;						
			}
			else{	
				SM_FileHandle file_handle;
				openPageFile(bm->pageFile,&file_handle);
				int pp=PAGE_SIZE;
				pf[0]=(Frame){
					.data=(SM_PageHandle) malloc(pp)
				};
				ensureCapacity(pageNum,&file_handle);
				readBlock(pageNum,&file_handle,pf[0].data);
				pf[0].fixCount=pf[0].fixCount+1+0;
				pf[0].pageNum=pageNum;				
				last_Index=0;
				hit_Count=0;
				pf[0].index=0;
				pf[0].hitNum=hit_Count;	
				p->data=pf[0].data;				
				p->pageNum=pageNum;				
				return Rcode;					
			}	
		}

	}	
}

extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	Frame *pf=(Frame *)(*bm).mgmtData;
	int size=sizeof(PageNumber)*buffer_Size;
	int temp=0;
	PageNumber *cont =malloc(size);
	int i=0;
	a:
		if(!(pf[i].pageNum <-1 || pf[i].pageNum > -1)){
			cont[i]=NO_PAGE;
		}
		else{
			if(temp==0)
				cont[i]=pf[i].pageNum;
		}
		i++;
	
	

	if(i<buffer_Size)
		goto a;

	return cont;
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	int i=0;
	int f=sizeof(bool)*buffer_Size;	
	Frame *pf = (Frame *)(*bm).mgmtData;
	bool *dirtyF = malloc(f);
		
	a:
	if(f!=0){
		if(pf[i].dirtySegment <1 || pf[i].dirtySegment>1){
			dirtyF[i]=false;
		}

		if(pf[i].dirtySegment==1){
			dirtyF[i] = true;
		}	
	}
	i++;

	if(i<buffer_Size)
		goto a;

	return dirtyF;
}

extern int *getFixCounts (BM_BufferPool *const bm)
{
	int buffer=0;
	int temp=0;
	buffer=sizeof(int)*buffer_Size;
	int i=0;
	Frame *pf= (Frame *)(*bm).mgmtData;
	int *fc = malloc(buffer);

	if(buffer>0){
		do{
			if(i<buffer_Size){
				if(!(pf[i].fixCount < -1 || pf[i].fixCount > -1)){
					if(temp==0)
						fc[i]=0;
				}
				else{
					fc[i]=pf[i].fixCount;
				}
			}
			i++;
		}while(i<buffer_Size);
	}

	return fc;
}

extern int getNumReadIO (BM_BufferPool *const bm)
{
	if(bm!=NULL){
		last_Index++;
		int temp=last_Index;
		if(true){
		return temp;
		}
	}
	else{
		return last_Index++;
	}
}

extern int getNumWriteIO (BM_BufferPool *const bm)
{
	int temp = count;
	if(true){
	return temp;
	}
}