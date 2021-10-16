rm -rf proj.2.2
submit_files=(
	'src/include/storage/page/b_plus_tree_page.h'
	'src/storage/page/b_plus_tree_page.cpp'
	'src/include/storage/page/b_plus_tree_internal_page.h'
	'src/storage/page/b_plus_tree_internal_page.cpp'
	'src/include/storage/page/b_plus_tree_leaf_page.h'
	'src/storage/page/b_plus_tree_leaf_page.cpp'
	'src/include/storage/index/b_plus_tree.h'
	'src/storage/index/b_plus_tree.cpp'
	'src/include/storage/index/index_iterator.h'
	'src/storage/index/index_iterator.cpp'
	'src/buffer/lru_replacer.cpp'
	'src/buffer/buffer_pool_manager.cpp'
	'src/include/buffer/lru_replacer.h'
	'src/include/buffer/buffer_pool_manager.h'
	'src/include/storage/index/index_iterator.h'
	'src/storage/index/index_iterator.cpp'
)
mkdir -p proj.2.2/src/storage/page
mkdir -p proj.2.2/src/storage/index
mkdir -p proj.2.2/src/buffer
mkdir -p proj.2.2/src/include/storage/page
mkdir -p proj.2.2/src/include/storage/index
mkdir -p proj.2.2/src/include/buffer
for file in ${submit_files[*]}; do
	cp $file proj.2.2/$file
done
