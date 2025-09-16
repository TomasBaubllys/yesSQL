// Hello everybody
// LsmTree
// To do:
// <key><value> pair
// Wal iraso struktura [[recordLength][key][valueLength][value][checksum]]
// WAL failo rasymas
// WAL failo isvalymas
// Storage iraso struktura [[recordLength][tombstoneFlag][key][valueLength][value][checksum]]
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