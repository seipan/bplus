package btree

type (
	Defaultdb struct {
		mp map[string]string
	}
)

func NewDefaultdb() *Defaultdb {
	return &Defaultdb{mp: make(map[string]string)}
}

func (db *Defaultdb) Get(key string) (string, bool) {
	value, ok := db.mp[key]
	return value, ok
}

func (db *Defaultdb) Set(key string, value string) {
	db.mp[key] = value
}

func (db *Defaultdb) Delete(key string) {
	delete(db.mp, key)
}

func (db *Defaultdb) GetValue(value string) (string, bool) {
	for _, value := range db.mp {
		if value == value {
			return value, true
		}
	}
	return "", false
}

func (db *Defaultdb) Close() {
	db.mp = nil
}

func (db *Defaultdb) Len() int {
	return len(db.mp)
}

func (db *Defaultdb) Keys() []string {
	keys := make([]string, 0, len(db.mp))
	for key := range db.mp {
		keys = append(keys, key)
	}
	return keys
}
