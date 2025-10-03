#ifndef SS_TABLE_CONTROLLER_H_INCLUDED
#define SS_TABLE_CONTROLLER_H_INCLUDED

#include <vector>
#include <memory>
#include <string>
#include "ss_table.h"

class SS_Table_Controller{
    private:
        std::vector<std::shared_ptr<SS_Table>> sstables;


};


#endif // SS_TABLE_CONTROLLER_H_INCLUDED
