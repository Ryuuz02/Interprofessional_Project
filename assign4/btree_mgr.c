#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "btree_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
// Function prototypes
void insertKey_insertInLeafOrSplit(BTreeHandle *, RM_BtreeNode *, Value *, RID);
int deleteKey_searchKeyInLeaf(RM_BtreeNode *, Value *);


RM_BtreeNode *root       = NULL;
int numNodValue          = 0;
int sizeofNod            = 0;
int globalPos            = 0;

void walkSubNodes(RM_BtreeNode *bTreeNode, char *result);
RC insertParent(RM_BtreeNode *left, RM_BtreeNode *right, Value key);

char *sv  = NULL;
char *sv2 = NULL;

Value empty;



void createNewNod_setGlobalPos(RM_BtreeNode *thisNode)
{
    if (thisNode != NULL)
        globalPos = 0;
    else
        globalPos = -1;
}


RM_BtreeNode *createNewNod_allocateNode()
{
    RM_BtreeNode *bTreeNode = (RM_BtreeNode *)malloc(sizeof(RM_BtreeNode));

    if (bTreeNode != NULL)
    {
        bTreeNode->ptrs = (void **)malloc(sizeof(void *) * sizeofNod);
        bTreeNode->keys = (Value *)malloc(sizeof(Value) * (sizeofNod - 1));
    }

    return bTreeNode;
}


bool strNoLarger(char *c1, char *c2)
{
    size_t l1 = strlen(c1);
    size_t l2 = strlen(c2);

    if (l1 < l2)
        return true;

    if (l1 > l2)
        return false;

    return strcmp(c1, c2) <= 0;
}


void createNewNod_initializeNode(RM_BtreeNode *bTreeNode)
{
    if (bTreeNode == NULL)
        return;

    bTreeNode->paptr = NULL;
    bTreeNode->KeyCounts = 0;
    bTreeNode->isLeaf = false;

    ++numNodValue;
}

RM_BtreeNode *createNewNod(RM_BtreeNode *thisNode)
{
    createNewNod_setGlobalPos(thisNode);

    RM_BtreeNode *bTreeNode = NULL;
    bTreeNode = createNewNod_allocateNode();

    if (bTreeNode != NULL)
        createNewNod_initializeNode(bTreeNode);

    return bTreeNode;
}

RC insertParent_splitNode(RM_BtreeNode *paptr, RM_BtreeNode *left, RM_BtreeNode *right, Value key, int index)
{
    int i = 0;
    int middleLoc = (sizeofNod % 2 == 0) ? (sizeofNod / 2) : (sizeofNod / 2 + 1);

    RM_BtreeNode **tempNode = (RM_BtreeNode **)malloc((sizeofNod + 1) * sizeof(RM_BtreeNode *));
    Value *tempKeys = (Value *)malloc(sizeof(Value) * sizeofNod);

    for (i = 0; i < sizeofNod + 1; ++i)
    {
        if (i < index + 1)
            tempNode[i] = paptr->ptrs[i];
        else if (i == index + 1)
            tempNode[i] = right;
        else
            tempNode[i] = paptr->ptrs[i - 1];
    }

    for (i = 0; i < sizeofNod; ++i)
    {
        if (i < index)
            tempKeys[i] = paptr->keys[i];
        else if (i == index)
            tempKeys[i] = key;
        else
            tempKeys[i] = paptr->keys[i - 1];
    }

    paptr->KeyCounts = middleLoc - 1;

    int j = 0;
    while (j < middleLoc - 1)
    {
        paptr->ptrs[j] = tempNode[j];
        paptr->keys[j] = tempKeys[j];
        ++j;
    }

    paptr->ptrs[j] = tempNode[j];

    RM_BtreeNode *newNode = createNewNod(NULL);
    newNode->KeyCounts = sizeofNod - middleLoc;

    for (i = middleLoc; i <= sizeofNod; ++i)
    {
        newNode->ptrs[i - middleLoc] = tempNode[i];
        newNode->keys[i - middleLoc] = (i < sizeofNod) ? tempKeys[i] : empty;
    }

    newNode->paptr = paptr->paptr;

    Value middleKey = tempKeys[middleLoc - 1];

    free(tempNode);
    free(tempKeys);

    return insertParent(paptr, newNode, middleKey);
}

RC insertParent_createNewRoot(RM_BtreeNode *left, RM_BtreeNode *right, Value key)
{
    RM_BtreeNode *NewRoot = createNewNod(NULL);

    if (NewRoot != NULL)
    {
        NewRoot->keys[0] = key;
        NewRoot->KeyCounts = 1;

        NewRoot->ptrs[0] = left;
        NewRoot->ptrs[1] = right;

        if (left != NULL)
            left->paptr = NewRoot;

        if (right != NULL)
            right->paptr = NewRoot;

        root = NewRoot;
    }

    return RC_OK;
}

int insertParent_findIndex(RM_BtreeNode *paptr, RM_BtreeNode *left)
{
    int index = 0;

    for (; index < paptr->KeyCounts; index++)
    {
        if (paptr->ptrs[index] == left)
            break;
    }

    return index;
}

RC insertParent_insertIntoParent(RM_BtreeNode *paptr, RM_BtreeNode *right, Value key, int index)
{
    int j = paptr->KeyCounts;

    for (; j > index; --j)
    {
        paptr->keys[j] = paptr->keys[j - 1];
        paptr->ptrs[j + 1] = paptr->ptrs[j];
    }

    paptr->keys[index] = key;
    paptr->ptrs[index + 1] = right;
    paptr->KeyCounts++;

    return RC_OK;
}

RC insertParent(RM_BtreeNode *leftNode, RM_BtreeNode *rightNode, Value newKey)
{
    RM_BtreeNode *parent = leftNode->paptr;

    // If there's no parent, we're at the root level
    if (!parent)
    {
        return insertParent_createNewRoot(leftNode, rightNode, newKey);
    }

    // Determine where the leftNode fits among the parent's pointers
    int position = insertParent_findIndex(parent, leftNode);

    // Either insert directly or split the parent if full
    bool hasRoom = (parent->KeyCounts < (sizeofNod - 1));
    if (hasRoom)
    {
        return insertParent_insertIntoParent(parent, rightNode, newKey, position);
    }

    // If full, split the parent and promote a key
    return insertParent_splitNode(parent, leftNode, rightNode, newKey, position);
}


RC deleteNode(RM_BtreeNode *bTreeNode, int index)
{
    RM_BtreeNode *brother = NULL;
    int position = 0;
    int NumKeys = 0;
    int i=0;
    int j = 0;
    

    bTreeNode->KeyCounts--;
    NumKeys = bTreeNode->KeyCounts;

    if (bTreeNode->isLeaf)
    {
        free(bTreeNode->ptrs[index]);
        bTreeNode->ptrs[index] = NULL;

        for (i = index; i < NumKeys; i++)
        {
            bTreeNode->keys[i] = bTreeNode->keys[i + 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i] = bTreeNode->ptrs[i + 1];
        }

        bTreeNode->keys[NumKeys] = empty;
        bTreeNode->ptrs[NumKeys] = NULL;
    }
    else
    {
        for (i = index - 1; i < NumKeys; i++)
        {
            bTreeNode->keys[i] = bTreeNode->keys[i + 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i + 1] = bTreeNode->ptrs[i + 2];
        }

        bTreeNode->keys[NumKeys] = empty;
        bTreeNode->ptrs[NumKeys + 1] = NULL;
    }

    int minKeys = bTreeNode->isLeaf ? sizeofNod / 2 : (sizeofNod - 1) / 2;
    if (NumKeys >= minKeys)
        return RC_OK;

    if (bTreeNode == root)
    {
        if (root->KeyCounts > 0)
            return RC_OK;

        RM_BtreeNode *newRoot = NULL;
        if (!root->isLeaf)
        {
            newRoot = root->ptrs[0];
            newRoot->paptr = NULL;
        }

        free(root->keys);
        root->keys = NULL;
        free(root->ptrs);
        root->ptrs = NULL;
        free(root);
        root = NULL;
        numNodValue--;
        root = newRoot;

        return RC_OK;
    }

    RM_BtreeNode *parentNode = bTreeNode->paptr;
    while (position < parentNode->KeyCounts && parentNode->ptrs[position] != bTreeNode)
        position++;

    brother = (position == 0) ? parentNode->ptrs[1] : parentNode->ptrs[position - 1];

    int maxMergeKeys = bTreeNode->isLeaf ? sizeofNod - 1 : sizeofNod - 2;

    if (brother->KeyCounts + NumKeys <= maxMergeKeys)
    {
        if (position == 0)
        {
            RM_BtreeNode *temp = bTreeNode;
            bTreeNode = brother;
            brother = temp;
            position = 1;
            NumKeys = bTreeNode->KeyCounts;
        }

        i = brother->KeyCounts;
        if (!bTreeNode->isLeaf)
        {
            brother->keys[i] = parentNode->keys[position - 1];
            i++;
            NumKeys++;
        }

        for (j = 0; j < NumKeys; j++, i++)
        {
            brother->keys[i] = bTreeNode->keys[j];
            globalPos = brother->pos;
            brother->ptrs[i] = bTreeNode->ptrs[j];
            bTreeNode->keys[j] = empty;
            bTreeNode->ptrs[j] = NULL;
        }

        brother->KeyCounts += NumKeys;
        brother->ptrs[sizeofNod - 1] = bTreeNode->ptrs[sizeofNod - 1];

        numNodValue--;

        free(bTreeNode->keys);
        bTreeNode->keys = NULL;
        free(bTreeNode->ptrs);
        bTreeNode->ptrs = NULL;
        free(bTreeNode);
        bTreeNode = NULL;

        return deleteNode(parentNode, position);
    }

    int brotherNumKeys = 0;

    if (position != 0)
    {
        if (!bTreeNode->isLeaf)
            bTreeNode->ptrs[NumKeys + 1] = bTreeNode->ptrs[NumKeys];

        for (i = NumKeys; i > 0; i--)
        {
            bTreeNode->keys[i] = bTreeNode->keys[i - 1];
            globalPos = bTreeNode->pos;
            bTreeNode->ptrs[i] = bTreeNode->ptrs[i - 1];
        }

        if (bTreeNode->isLeaf)
        {
            brotherNumKeys = brother->KeyCounts - 1;
            bTreeNode->keys[0] = brother->keys[brotherNumKeys];
            parentNode->keys[position - 1] = bTreeNode->keys[0];
        }
        else
        {
            brotherNumKeys = brother->KeyCounts;
            bTreeNode->keys[0] = parentNode->keys[position - 1];
            parentNode->keys[position - 1] = brother->keys[brotherNumKeys - 1];
        }

        bTreeNode->ptrs[0] = brother->ptrs[brotherNumKeys];
        brother->keys[brotherNumKeys] = empty;
        brother->ptrs[brotherNumKeys] = NULL;
    }
    else
    {
        int temp = brother->KeyCounts;

        if (bTreeNode->isLeaf)
        {
            bTreeNode->keys[NumKeys] = brother->keys[0];
            bTreeNode->ptrs[NumKeys] = brother->ptrs[0];
            parentNode->keys[0] = brother->keys[1];
        }
        else
        {
            bTreeNode->keys[NumKeys] = parentNode->keys[0];
            bTreeNode->ptrs[NumKeys + 1] = brother->ptrs[0];
            parentNode->keys[0] = brother->keys[0];
        }

        for (i = 0; i < temp; i++)
        {
            brother->keys[i] = brother->keys[i + 1];
            globalPos = brother->KeyCounts;
            brother->ptrs[i] = brother->ptrs[i + 1];
        }

        brother->ptrs[brother->KeyCounts] = NULL;
        brother->keys[brother->KeyCounts] = empty;
    }

    bTreeNode->KeyCounts++;
    brother->KeyCounts--;

    return RC_OK;
}


RC initIndexManager(void *mgmtData)
{
    root = NULL;
    numNodValue = 0;
    sizeofNod = 0;

    empty.dt = DT_INT;
    empty.v.intV = 0;

    return RC_OK;
}


RC shutdownIndexManager()
{
    // No shutdown logic needed
    return RC_OK;
}

RC createBtree(char *idxId, DataType keyType, int n)
{
    if (idxId == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RC rc;
    SM_FileHandle fhandle;
    SM_PageHandle pageData;

    rc = createPageFile(idxId);
    if (rc != RC_OK)
        return rc;

    rc = openPageFile(idxId, &fhandle);
    if (rc != RC_OK)
        return rc;

    pageData = (SM_PageHandle)malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RC_WRITE_FAILED;

    memcpy(pageData, &keyType, sizeof(int));
    pageData += sizeof(int);
    memcpy(pageData, &n, sizeof(int));
    pageData -= sizeof(int);

    rc = writeCurrentBlock(&fhandle, pageData);
    if (rc != RC_OK)
    {
        free(pageData);
        return rc;
    }

    rc = closePageFile(&fhandle);
    if (rc != RC_OK)
    {
        free(pageData);
        return rc;
    }

    free(pageData);
    return RC_OK;
}


RC openBtree(BTreeHandle **tree, char *idxId)
{
    if (idxId == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RC rc;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    *tree = (BTreeHandle *)malloc(sizeof(BTreeHandle));
    if (*tree == NULL)
        return RC_WRITE_FAILED;

    rc = initBufferPool(bm, idxId, 10, RS_CLOCK, NULL);
    if (rc != RC_OK)
        return rc;

    rc = pinPage(bm, page, 0);
    if (rc != RC_OK)
        return rc;

    int type;
    memcpy(&type, page->data, sizeof(int));
    (*tree)->keyType = (DataType)type;

    page->data += sizeof(int);
    int n;
    memcpy(&n, page->data, sizeof(int));
    page->data -= sizeof(int);

    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)malloc(sizeof(RM_bTree_mgmtData));
    bTreeMgmt->numEntries = 0;
    bTreeMgmt->maxKeyNum = n;
    bTreeMgmt->bp = bm;

    (*tree)->mgmtData = bTreeMgmt;

    free(page);
    page = NULL;

    return RC_OK;
}


RC closeBtree(BTreeHandle *tree)
{
    if (tree == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RC rc;

    tree->idxId = NULL;
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;

    rc = shutdownBufferPool(bTreeMgmt->bp);
    if (rc != RC_OK)
        return rc;

    free(bTreeMgmt);
    bTreeMgmt = NULL;

    free(tree);
    tree = NULL;

    if (root != NULL)
    {
        free(root);
        root = NULL;
    }

    return RC_OK;
}


RC deleteBtree(char *idxId)
{
    if (idxId == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RC rc = destroyPageFile(idxId);
    if (rc != RC_OK)
        return rc;

    return RC_OK;
}


RC getNumNodes(BTreeHandle *tree, int *result)
{
    if (tree == NULL)
        return RC_IM_KEY_NOT_FOUND;

    *result = numNodValue;
    return RC_OK;
}


RC getNumEntries(BTreeHandle *tree, int *result)
{
    if (tree == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_bTree_mgmtData *meta = (RM_bTree_mgmtData *)tree->mgmtData;
    *result = meta->numEntries;

    return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result)
{
    if (tree == NULL)
        return RC_IM_KEY_NOT_FOUND;

    *result = tree->keyType;
    return RC_OK;
}


RM_BtreeNode *findKey_locateLeafNode(BTreeHandle *tree, Value *key)
{
    RM_BtreeNode *leaf = root;
    int i = 0;

    while (leaf != NULL && !leaf->isLeaf)
    {
        while (i < leaf->KeyCounts)
        {
            sv = serializeValue(&leaf->keys[i]);
            sv2 = serializeValue(key);

            if (strNoLarger(sv, sv2))
            {
                free(sv);
                sv = NULL;
                i++;
                if (i < leaf->KeyCounts)
                    sv = serializeValue(&leaf->keys[i]);
            }
            else
            {
                break;
            }

            free(sv2);
            sv2 = NULL;
        }

        if (sv != NULL)
        {
            free(sv);
            sv = NULL;
        }

        leaf = (RM_BtreeNode *)leaf->ptrs[i];
        i = 0;
    }

    return leaf;
}


int findKey_searchInLeaf(RM_BtreeNode *leaf, Value *key)
{
    int i = 0;

    sv = serializeValue(&leaf->keys[i]);
    sv2 = serializeValue(key);

    while (i < leaf->KeyCounts && strcmp(sv, sv2) != 0)
    {
        free(sv);
        sv = NULL;

        i++;
        if (i < leaf->KeyCounts)
            sv = serializeValue(&leaf->keys[i]);
    }

    free(sv);
    sv = NULL;
    free(sv2);
    sv2 = NULL;

    return i;
}

RC findKey_setResultIfFound(RM_BtreeNode *leaf, int keyIndex, RID *result)
{
    if (keyIndex >= leaf->KeyCounts)
        return RC_IM_KEY_NOT_FOUND;

    RID *ridPtr = (RID *)leaf->ptrs[keyIndex];
    result->page = ridPtr->page;
    result->slot = ridPtr->slot;

    return RC_OK;
}


RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    if (tree == NULL || key == NULL || root == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_BtreeNode *leaf = findKey_locateLeafNode(tree, key);
    int keyIndex = findKey_searchInLeaf(leaf, key);

    return findKey_setResultIfFound(leaf, keyIndex, result);
}

void insertKey_insertWithoutSplit(RM_BtreeNode *leaf, Value *key, RID rid, int index)
{
    int i = leaf->KeyCounts;

    while (i > index)
    {
        leaf->keys[i] = leaf->keys[i - 1];
        globalPos = leaf->pos;
        leaf->ptrs[i] = leaf->ptrs[i - 1];
        i--;
    }

    RID *rec = (RID *)malloc(sizeof(RID));
    rec->page = rid.page;
    rec->slot = rid.slot;

    leaf->keys[index] = *key;
    leaf->ptrs[index] = rec;
    leaf->KeyCounts++;
}

int insertKey_findPositionInLeaf(RM_BtreeNode *leaf, Value *key)
{
    int index = 0;

    sv = serializeValue(&leaf->keys[index]);
    sv2 = serializeValue(key);

    while (index < leaf->KeyCounts && strNoLarger(sv, sv2))
    {
        free(sv);
        sv = NULL;
        index++;

        if (index < leaf->KeyCounts)
            sv = serializeValue(&leaf->keys[index]);
    }

    free(sv);
    sv = NULL;
    free(sv2);
    sv2 = NULL;

    return index;
}

RM_BtreeNode *insertKey_findLeaf(BTreeHandle *tree, Value *key)
{
    RM_BtreeNode *leaf = root;
    int i = 0;

    while (leaf != NULL && !leaf->isLeaf)
    {
        while (i < leaf->KeyCounts && strNoLarger(serializeValue(&leaf->keys[i]), serializeValue(key)))
        {
            sv = serializeValue(&leaf->keys[i]);
            sv2 = serializeValue(key);

            if (strNoLarger(sv, sv2))
            {
                free(sv);
                sv = NULL;
                i++;
                if (i < leaf->KeyCounts)
                    sv = serializeValue(&leaf->keys[i]);
            }
            else
            {
                break;
            }

            free(sv2);
            sv2 = NULL;
        }

        if (sv != NULL)
        {
            free(sv);
            sv = NULL;
        }

        leaf = (RM_BtreeNode *)leaf->ptrs[i];
        i = 0;
    }

    return leaf;
}



void insertKey_initializeRoot(BTreeHandle *tree, Value *key, RID rid)
{
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    sizeofNod = bTreeMgmt->maxKeyNum + 1;

    root = createNewNod(root);

    RID *rec = (RID *)malloc(sizeof(RID));
    rec->page = rid.page;
    rec->slot = rid.slot;

    root->ptrs[0] = rec;
    root->keys[0] = *key;
    root->ptrs[sizeofNod - 1] = NULL;
    root->isLeaf = true;
    root->KeyCounts += 1;
}

void insertKey_splitAndInsert(BTreeHandle *tree, RM_BtreeNode *leaf, Value *key, RID rid, int index)
{
    RM_BtreeNode *newLeafNode = createNewNod(NULL);
    RID **NodeRID = malloc(sizeof(RID *) * sizeofNod);
    Value *NodeKeys = malloc(sizeof(Value) * sizeofNod);

    int i = 0;
    int middleLoc = 0;

    while (i < sizeofNod)
    {
        if (i == index)
        {
            NodeRID[i] = (RID *)malloc(sizeof(RID));
            NodeRID[i]->page = rid.page;
            NodeRID[i]->slot = rid.slot;
            NodeKeys[i] = *key;
        }
        else if (i < index)
        {
            NodeRID[i] = leaf->ptrs[i];
            NodeKeys[i] = leaf->keys[i];
        }
        else
        {
            NodeRID[i] = leaf->ptrs[i - 1];
            NodeKeys[i] = leaf->keys[i - 1];
        }
        i++;
    }

    middleLoc = (sizeofNod / 2) + 1;

    for (i = 0; i < middleLoc; i++)
    {
        leaf->ptrs[i] = NodeRID[i];
        leaf->keys[i] = NodeKeys[i];
    }

    newLeafNode->isLeaf = true;
    newLeafNode->paptr = leaf->paptr;
    newLeafNode->KeyCounts = sizeofNod - middleLoc;

    for (i = middleLoc; i < sizeofNod; i++)
    {
        newLeafNode->ptrs[i - middleLoc] = NodeRID[i];
        newLeafNode->keys[i - middleLoc] = NodeKeys[i];
    }

    newLeafNode->ptrs[sizeofNod - 1] = leaf->ptrs[sizeofNod - 1];
    leaf->KeyCounts = middleLoc;
    leaf->ptrs[sizeofNod - 1] = newLeafNode;

    free(NodeRID);
    free(NodeKeys);

    insertParent(leaf, newLeafNode, newLeafNode->keys[0]);
}





RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
    if (tree == NULL || key == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_BtreeNode *leaf = insertKey_findLeaf(tree, key);

    if (leaf == NULL)
    {
        insertKey_initializeRoot(tree, key, rid);
    }
    else
    {
        insertKey_insertInLeafOrSplit(tree, leaf, key, rid);
    }

    RM_bTree_mgmtData *meta = (RM_bTree_mgmtData *)tree->mgmtData;
    meta->numEntries++;

    return RC_OK;
}


RM_BtreeNode *deleteKey_findLeafNode(BTreeHandle *tree, Value *key)
{
    RM_BtreeNode *leaf = root;
    int i = 0;

    while (leaf != NULL && !leaf->isLeaf)
    {
        while (i < leaf->KeyCounts && strNoLarger(serializeValue(&leaf->keys[i]), serializeValue(key)))
        {
            sv = serializeValue(&leaf->keys[i]);
            sv2 = serializeValue(key);

            if (strNoLarger(sv, sv2))
            {
                free(sv);
                sv = NULL;
                i++;

                if (i < leaf->KeyCounts)
                    sv = serializeValue(&leaf->keys[i]);
            }
            else
            {
                break;
            }

            free(sv2);
            sv2 = NULL;
        }

        if (sv != NULL)
        {
            free(sv);
            sv = NULL;
        }

        leaf = (RM_BtreeNode *)leaf->ptrs[i];
        i = 0;
    }

    return leaf;
}

void insertKey_insertInLeafOrSplit(BTreeHandle *tree, RM_BtreeNode *leaf, Value *key, RID rid)
{
    int index = insertKey_findPositionInLeaf(leaf, key);

    if (leaf->KeyCounts < (sizeofNod - 1))
    {
        insertKey_insertWithoutSplit(leaf, key, rid, index);
    }
    else
    {
        insertKey_splitAndInsert(tree, leaf, key, rid, index);
    }
}



RC deleteKey_deleteNodeFromLeaf(RM_BtreeNode *leaf, int index)
{
    RC status = deleteNode(leaf, index);
    return status;
}

RC deleteKey(BTreeHandle *tree, Value *key)
{
    if (tree == NULL || key == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    bTreeMgmt->numEntries--;

    RM_BtreeNode *leaf = deleteKey_findLeafNode(tree, key);
    int keyIndex = deleteKey_searchKeyInLeaf(leaf, key);

    if (keyIndex < leaf->KeyCounts)
    {
        RC rc = deleteKey_deleteNodeFromLeaf(leaf, keyIndex);
        if (rc != RC_OK)
            return rc;
    }

    tree->mgmtData = bTreeMgmt;
    return RC_OK;
}


void initializeScanMgmtData(BT_ScanHandle **handle)
{
    RM_BScan_mgmt *scanMgmt = (RM_BScan_mgmt *)malloc(sizeof(RM_BScan_mgmt));

    scanMgmt->cur = NULL;
    scanMgmt->index = 0;
    scanMgmt->totalScan = 0;

    (*handle)->mgmtData = scanMgmt;
}

void initializeScanHandle(BTreeHandle *tree, BT_ScanHandle **handle)
{
    BT_ScanHandle *scanHandle = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    scanHandle->tree = tree;
    *handle = scanHandle;

    initializeScanMgmtData(handle);
}


int deleteKey_searchKeyInLeaf(RM_BtreeNode *leaf, Value *key)
{
    int i = 0;

    sv = serializeValue(&leaf->keys[i]);
    sv2 = serializeValue(key);

    while (i < leaf->KeyCounts && strcmp(sv, sv2) != 0)
    {
        free(sv);
        sv = NULL;

        i++;
        if (i < leaf->KeyCounts)
            sv = serializeValue(&leaf->keys[i]);
    }

    free(sv);
    sv = NULL;
    free(sv2);
    sv2 = NULL;

    return i;
}


RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    if (tree == NULL)
        return RC_IM_KEY_NOT_FOUND;

    initializeScanHandle(tree, handle);
    return RC_OK;
}


void initializeLeafNode(RM_BScan_mgmt *scanMgmt)
{
    if (scanMgmt->totalScan == 0)
    {
        RM_BtreeNode *leaf = root;

        while (leaf != NULL && !leaf->isLeaf)
        {
            leaf = leaf->ptrs[0];
        }

        scanMgmt->cur = leaf;
    }
}

void moveToNextLeafIfNeeded(RM_BScan_mgmt *scanMgmt, BT_ScanHandle *handle)
{
    if (scanMgmt->index == scanMgmt->cur->KeyCounts)
    {
        int lastPtrIndex = ((RM_bTree_mgmtData *)handle->tree->mgmtData)->maxKeyNum;
        scanMgmt->cur = (RM_BtreeNode *)scanMgmt->cur->ptrs[lastPtrIndex];
        scanMgmt->index = 0;
    }
}

void retrieveNextRID(RM_BScan_mgmt *scanMgmt, RID *result)
{
    RID *ridRes = (RID *)scanMgmt->cur->ptrs[scanMgmt->index];
    scanMgmt->index++;
    scanMgmt->totalScan++;

    result->page = ridRes->page;
    result->slot = ridRes->slot;
}


RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    if (handle == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_BScan_mgmt *scanMgmt = (RM_BScan_mgmt *)handle->mgmtData;
    int totalRes = 0;

    RC rc = getNumEntries(handle->tree, &totalRes);
    if (rc != RC_OK)
        return rc;

    if (scanMgmt->totalScan >= totalRes)
        return RC_IM_NO_MORE_ENTRIES;

    initializeLeafNode(scanMgmt);
    moveToNextLeafIfNeeded(scanMgmt, handle);
    retrieveNextRID(scanMgmt, result);

    return RC_OK;
}


RC closeTreeScan(BT_ScanHandle *handle)
{
    if (handle == NULL)
        return RC_IM_KEY_NOT_FOUND;

    RM_BScan_mgmt *mgmt = (RM_BScan_mgmt *)handle->mgmtData;

    free(mgmt);
    mgmt = NULL;

    free(handle);
    handle = NULL;

    return RC_OK;
}

void processLeafNode(RM_BtreeNode *bTreeNode, char *line, char *t)
{
    int i = 0;

    while (i < bTreeNode->KeyCounts)
    {
        sprintf(t, "%d.%d,", ((RID *)bTreeNode->ptrs[i])->page, ((RID *)bTreeNode->ptrs[i])->slot);
        strcat(line, t);

        sv = serializeValue(&bTreeNode->keys[i]);
        strcat(line, sv);
        free(sv);
        sv = NULL;

        strcat(line, ",");
        i++;
    }

    if (bTreeNode->ptrs[sizeofNod - 1] != NULL)
    {
        int nextPos = ((RM_BtreeNode *)bTreeNode->ptrs[sizeofNod - 1])->pos;
        sprintf(t, "%d", nextPos);
        strcat(line, t);
    }
    else
    {
        line[strlen(line) - 1] = '\0'; // Remove trailing comma
    }

    strcat(line, "]\n");
}

int DFS(RM_BtreeNode *bTreeNode)
{
    bTreeNode->pos = globalPos++;

    if (bTreeNode->isLeaf)
        return 0;

    for (int i = 0; i <= bTreeNode->KeyCounts; i++)
    {
        DFS(bTreeNode->ptrs[i]);
    }

    return 0;
}

char *initializeLine(RM_BtreeNode *bTreeNode)
{
    char *line = (char *)malloc(100);
    char *temp = (char *)malloc(10);

    strcpy(line, "(");
    sprintf(temp, "%d", bTreeNode->pos);
    strcat(line, temp);
    strcat(line, ")[");

    free(temp);
    return line;
}



void processInternalNode(RM_BtreeNode *bTreeNode, char *line, char *t)
{
    int i = 0;

    while (i < bTreeNode->KeyCounts)
    {
        sprintf(t, "%d,", ((RM_BtreeNode *)bTreeNode->ptrs[i])->pos);
        strcat(line, t);

        sv = serializeValue(&bTreeNode->keys[i]);
        strcat(line, sv);
        free(sv);
        sv = NULL;

        strcat(line, ",");
        i++;
    }

    RM_BtreeNode *lastPtr = (RM_BtreeNode *)bTreeNode->ptrs[i];
    if (lastPtr != NULL)
    {
        sprintf(t, "%d", lastPtr->pos);
        strcat(line, t);
    }
    else
    {
        line[strlen(line) - 1] = '\0'; // Remove trailing comma
    }

    strcat(line, "]\n");
}

void processNode(RM_BtreeNode *bTreeNode, char *line, char *t)
{
    if (bTreeNode->isLeaf)
    {
        processLeafNode(bTreeNode, line, t);
    }
    else
    {
        processInternalNode(bTreeNode, line, t);
    }
}


int walk(RM_BtreeNode *currentNode, char *output)
{
    char *formatted = initializeLine(currentNode);
    char *buffer = (char *)malloc(10 * sizeof(char));

    processNode(currentNode, formatted, buffer);

    strcat(output, formatted);

    if (formatted != NULL)
    {
        free(formatted);
        formatted = NULL;
    }

    if (buffer != NULL)
    {
        free(buffer);
        buffer = NULL;
    }

    walkSubNodes(currentNode, output);

    return 0;
}




char *printTree(BTreeHandle *tree)
{
    if (root == NULL)
        return NULL;

    globalPos = 0;
    DFS(root);

    int length = 1000;  // Preallocated result buffer size
    char *result = (char *)malloc(length * sizeof(char));

    walk(root, result);

    return result;
}

void walkSubNodes(RM_BtreeNode *bTreeNode, char *result)
{
    if (!bTreeNode->isLeaf)
    {
        for (int i = 0; i <= bTreeNode->KeyCounts; i++)
        {
            walk(bTreeNode->ptrs[i], result);
        }
    }
}