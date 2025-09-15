#ifndef YSQL_BITS_H_INCLUDED
#define YSQL_BITS_H_INCLUDED

class Bits {
	private:
		uint64_t size;
	public:
		Bits(uint64_t &size): size(size) {
		
		}

	uint64_t size() {
		return this -> size;
	}
};

#endif
