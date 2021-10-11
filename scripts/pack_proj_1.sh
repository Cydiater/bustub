rm -rf proj.1
submit_files=('src/buffer/lru_replacer.cpp' 'src/buffer/buffer_pool_manager.cpp' 'src/include/buffer/lru_replacer.h' 'src/include/buffer/buffer_pool_manager.h')
mkdir -p proj.1/src/buffer
mkdir -p proj.1/src/include/buffer
for file in ${submit_files[*]}; do
	cp $file proj.1/$file
done
