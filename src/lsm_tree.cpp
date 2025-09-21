#include "../include/lsm_tree.h"


LsmTree::LsmTree(){
}

LsmTree::~LsmTree(){
}
// LsmTree
// To do:
// WAL failo rasymas
// WAL failo isvalymas
// MemTable rasymas
// MemTable sortinimas
// MemTable skaitymas
// SSTable rasymas is MemTable
// SSTable skaitymas
// GET <key>
// SET <key> <value>
// Multithread SET method to write into WAL and MemTable at the same time
// GETKEYS - grąžina visus raktus
// GETKEYS <prefix> - grąžina visus raktus su duota pradžia
// GETFF <key> - gauti raktų reikšmių poras nuo key
// GETFB <key> - gauti raktų reikšmių poras nuo key atvirkščia tvarka
// GETFF ir GETFB turi leisti puslapiuoti, t.y. gauti po n porų ir fektyviai tęsti toliau
// REMOVE <key>

