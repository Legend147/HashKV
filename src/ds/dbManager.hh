#ifndef __DB_MANAGER_HH__
#define __DB_MANAGER_HH__

template<class T> class DBManager {
public:
    DBManager() {};
    virtual ~DBManager() {};

    virtual const char* setValue (char *keyStr, T) = 0;
    virtual T getValue (char *keyStr) = 0;
    virtual T removeValue (char *keyStr) = 0;
    virtual void listKeys(FILE *out = stdout, bool countOnly = false) = 0;

};
#endif
