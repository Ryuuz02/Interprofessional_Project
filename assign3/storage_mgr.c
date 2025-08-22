#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<string.h>
#include<unistd.h>
#include<math.h>
#include "storage_mgr.h"
#include "dberror.h"
#include "helper.h"
#define RC_FAILED_DEL 10

FILE *filepointer;
FILE *filename;
RC return_code;


void initStorageManager (void)
{
	//printf("Initializing the Storage Manager");
}

RC createPageFile (char *fileName){
	char *tpg_str, *f_pg;
	tpg_str = (char *) calloc(PAGE_SIZE, sizeof(char)); 
	f_pg = (char *) calloc(PAGE_SIZE, sizeof(char));  
	FILE *pgf = fopen(fileName, "w");
	int temp=1,temp1=0,temp2=0;
	if(pgf==NULL)                                            
    {
		return RC_FILE_NOT_FOUND;
	}
    else
    {
		if(temp==1){
			temp1=1;
		}
		if(temp1==1){
			strcat(tpg_str,"1\n");
			fwrite(tpg_str, sizeof(char), PAGE_SIZE, pgf);
			temp2=1;
			fwrite(f_pg, sizeof(char), PAGE_SIZE, pgf);			
		}
		if(temp2==1){
			free(tpg_str);
			free(f_pg);
			temp=0;
			fclose(pgf);
		}
		return RC_OK;
	}
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    FILE *pgf = fopen(fileName, "r+");
	int temp=1,temp1=0,temp2=0;
	if(fileName==NULL){
		temp = 0;
		return RC_FILE_NOT_FOUND;
	}
    return_code=RC_OK;
	char *var = (char *) calloc(PAGE_SIZE, sizeof(char));
	if(temp==1){		 
	   temp1=1;
       fgets(var, PAGE_SIZE, pgf);
    }
	var = strtok(var, "\n");
	if(temp1==1){		
		temp2=1;
	}
	if(temp2==1){
		fHandle->totalNumPages = atoi(var);
		fHandle->fileName = fileName;         
        fHandle->curPagePos = 0;
		fHandle->mgmtInfo = pgf;
        free(var);
	}
	return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle){
	int temp=0;
	if(temp!=1){
		if(!fclose(fHandle->mgmtInfo)){
			temp=0;
		}
	}
	if(temp==0){
		return RC_OK;
	}
	else
  	return RC_FILE_NOT_FOUND;
}

RC destroyPageFile (char *Fname)
{
	char *mblock=malloc(PAGE_SIZE * sizeof(char)); 
	if(remove(Fname)<0 || remove(Fname)>0)
	return_code=RC_FAILED_DEL;		
	else
	return_code=RC_OK;
    free(mblock);
	return return_code;
}


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	return_code = RC_OK;
	int temp=1,temp1=0,temp2=0;
	int ss,sp;    

	if(temp==1){
		if(pageNum > (*fHandle).totalNumPages){
			return_code =  RC_READ_NON_EXISTING_PAGE;
			temp=0;
		}
		else if(pageNum < 0){
			temp=0;
			return_code =  RC_READ_NON_EXISTING_PAGE;
		}
	}
	if(temp==0){
		return return_code;
	}
	else{
		temp1=1;
	}

	if(temp1==1){
    	sp = (pageNum+1)*PAGE_SIZE*sizeof(char);
		temp2=1;
	}

	if(temp2==1)
    	ss = fseek((*fHandle).mgmtInfo, sp, SEEK_SET);

	if(!(ss<0||ss>0)){
		if(temp1==1)
		 	fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);
		fHandle->curPagePos=pageNum;
		return return_code;
	}

	return RC_READ_NON_EXISTING_PAGE;
}

int getBlockPos (SM_FileHandle *fHandle)
{
	if(fHandle == NULL)
	return RC_FILE_NOT_FOUND;                                          
    else
   	return ((*fHandle).curPagePos);     	
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int temp=1;
	if(fHandle != NULL)     
	return RC_FILE_NOT_FOUND;
    else{
    	if(temp)
    		return(readBlock (0,fHandle,memPage)); 
		else
		   return RC_FILE_NOT_FOUND;    
	}
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int temp=0;
	if(!(*fHandle).fileName)
	return RC_FILE_NOT_FOUND;
	int prev_b=0;
	if(!temp)
	prev_b=(*fHandle).curPagePos-1;
	else
	prev_b=-1;
	return(readBlock (prev_b, fHandle, memPage));
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int temp = (fHandle==NULL)? 0: 1;
	if(temp)
	return (readBlock((*fHandle).curPagePos,fHandle,memPage));
	else
	return RC_FILE_NOT_FOUND;	
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int temp = (fHandle==NULL)? 1 : 0;
	if(!temp){
	int nBlock=(*fHandle).curPagePos+1;
	return(readBlock(nBlock,fHandle,memPage));
	}
	else
	return RC_FILE_NOT_FOUND;	
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int lBlock=(*fHandle).totalNumPages-1;
	int temp = (fHandle==NULL)? 0: 1;

	if(temp)
	return RC_FILE_NOT_FOUND;

	if(!temp)
		return(readBlock (lBlock,fHandle,memPage));

	return RC_FILE_NOT_FOUND;
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

    int ss;
	int temp=1,temp2=0,temp3=0;
    int sp;
    
    return_code = RC_OK;
	if(temp==1){
		if(pageNum > (*fHandle).totalNumPages){
			return_code =  RC_READ_NON_EXISTING_PAGE;
			temp=0;
		}
		else if(pageNum < 0){
			temp=0;
			return_code =  RC_READ_NON_EXISTING_PAGE;
		}
	}

	if(temp==0){
		return return_code;
	}
	else{
		temp2=1;
	}

	if(temp2==1){
		sp=(pageNum+1)*PAGE_SIZE*sizeof(char);
		temp3=1;
	}

	if(temp3==1){
		ss = fseek((*fHandle).mgmtInfo, sp, SEEK_SET); 
	}
    
	if(!(ss<0||ss>0)){
		if(temp==1){
			int size = sizeof(char);
		 	fwrite(memPage,size,PAGE_SIZE,fHandle->mgmtInfo);
		}
		fHandle->curPagePos=pageNum;
		return return_code;
	}
    
	return RC_WRITE_FAILED;
}


extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return_code = RC_OK;
	int temp=(fHandle==NULL)? 0: 1;
	if(!temp)
	return RC_FILE_NOT_FOUND;
	else{
		filepointer=fopen((*fHandle).fileName,"r+");
		int curPos = 0;
		if(1){
		curPos=(*fHandle).curPagePos;
		appendEmptyBlock(fHandle);
		}
		fseek(filepointer,curPos,SEEK_SET);
		int size = sizeof(char);
		int length = strlen(memPage);
		fwrite(memPage,size,length,filepointer);
		curPos=ftell(filepointer);	
		fclose(filepointer);
	}	
	return return_code;
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
	int temp = (fHandle==NULL)? 0: 1;
	if(!temp)
	return RC_FILE_NOT_FOUND;
	else{
		int size = sizeof(char);
		int psize = PAGE_SIZE;
		SM_PageHandle eBlock = (SM_PageHandle)calloc(psize,size);
		if(fseek(filepointer,0,SEEK_END)<0||fseek(filepointer,0,SEEK_END)>0){
			free(eBlock);
			return_code = RC_WRITE_FAILED;
		}
		else			
		fwrite(eBlock,size,psize,filepointer);
		(*fHandle).totalNumPages++;
		free(eBlock);		
	}
	return return_code;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	int temp = (fHandle==NULL)?1:0;
	int nPages = numberOfPages;
	if (temp)
	return RC_FILE_HANDLE_NOT_INIT;
	else
    {
    int pgs=nPages-(*fHandle).totalNumPages;                   
    if((pgs == 0 || pgs >0))                                                          
    {
	int i=0;
	do{
		if(i<pgs)
		appendEmptyBlock(fHandle);
		i++;
	}while(i<pgs);                             
    return RC_OK;                
    }
    else
    return RC_WRITE_FAILED;
    }
}

