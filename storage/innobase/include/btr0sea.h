/*****************************************************************************

Copyright (c) 1996, 2016, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2017, 2022, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

/********************************************************************//**
@file include/btr0sea.h
The index tree adaptive search

Created 2/17/1996 Heikki Tuuri
*************************************************************************/

#pragma once

#include "dict0dict.h"
#ifdef BTR_CUR_HASH_ADAPT
# include "buf0buf.h"

# ifdef UNIV_PFS_RWLOCK
extern mysql_pfs_key_t btr_search_latch_key;
# endif /* UNIV_PFS_RWLOCK */

# define btr_search_sys_create() btr_search.create()
# define btr_search_sys_free() btr_search.free()

/** Tries to guess the right search position based on the hash search info
of the index. Note that if mode is PAGE_CUR_LE, which is used in inserts,
and the function returns TRUE, then cursor->up_match and cursor->low_match
both have sensible values.
@param[in,out]	index		index
@param[in]	tuple		logical record
@param[in]	mode		PAGE_CUR_L, ....
@param[in]	latch_mode	BTR_SEARCH_LEAF, ...
@param[out]	cursor		tree cursor
@param[in]	mtr		mini-transaction
@return whether the search succeeded */
bool
btr_search_guess_on_hash(
	dict_index_t*	index,
	const dtuple_t*	tuple,
	page_cur_mode_t	mode,
	btr_latch_mode	latch_mode,
	btr_cur_t*	cursor,
	mtr_t*		mtr) noexcept;

/** Move or delete hash entries for moved records, usually in a page split.
If new_block is already hashed, then any hash index for block is dropped.
If new_block is not hashed, and block is hashed, then a new hash index is
built to new_block with the same parameters as block.
@param[in,out]	new_block	destination page
@param[in,out]	block		source page (subject to deletion later) */
void btr_search_move_or_delete_hash_entries(buf_block_t *new_block,
                                            buf_block_t *block) noexcept;

/** Drop any adaptive hash index entries that point to an index page.
@param[in,out]	block	block containing index page, s- or x-latched, or an
			index page for which we know that
			block->buf_fix_count == 0 or it is an index page which
			has already been removed from the buf_pool.page_hash
			i.e.: it is in state BUF_BLOCK_REMOVE_HASH
@param[in]	garbage_collect	drop ahi only if the index is marked
				as freed */
void btr_search_drop_page_hash_index(buf_block_t* block,
				     bool garbage_collect) noexcept;

/** Drop possible adaptive hash index entries when a page is evicted
from the buffer pool or freed in a file, or the index is being dropped.
@param[in]	page_id		page id */
void btr_search_drop_page_hash_when_freed(const page_id_t page_id) noexcept;

/** Update the page hash index after a single record is inserted on a page.
@param cursor cursor which was positioned before the inserted record */
void btr_search_update_hash_node_on_insert(btr_cur_t *cursor) noexcept;

/** Update the page hash index after a single record is inserted on a page.
@param cursor cursor which was positioned before the inserted record */
void btr_search_update_hash_on_insert(btr_cur_t *cursor) noexcept;

/** Updates the page hash index before a single record is deleted from a page.
@param cursor   cursor positioned on the to-be-deleted record */
void btr_search_update_hash_on_delete(btr_cur_t *cursor) noexcept;

/** Validates the search system.
@param thd   connection, for checking if CHECK TABLE has been killed
@return true if ok */
bool btr_search_validate(THD *thd) noexcept;

# ifdef UNIV_DEBUG
/** @return if the index is marked as freed */
bool btr_search_check_marked_free_index(const buf_block_t *block) noexcept;
# endif /* UNIV_DEBUG */

struct ahi_node;

/** The hash index system */
struct btr_sea
{
  /** the actual value of innodb_adaptive_hash_index */
  Atomic_relaxed<bool> enabled;

  /** Disable the adaptive hash search system and empty the index. */
  void disable() noexcept;

  /** Enable the adaptive hash search system.
  @param resize whether buf_pool_t::resize() is the caller */
  void enable(bool resize= false) noexcept;

  /** Partition of the hash table */
  struct partition
  {
    /** latch protecting the hash table */
    alignas(CPU_LEVEL1_DCACHE_LINESIZE) srw_spin_lock latch;
    /** map of CRC-32C of rec prefix to rec_t* in buf_page_t::frame */
    hash_table_t table;
    /** latch protecting blocks, spare */
    srw_mutex blocks_mutex;
    /** allocated blocks */
    UT_LIST_BASE_NODE_T(buf_page_t) blocks;
    /** a cached block to extend blocks */
    Atomic_relaxed<buf_block_t*> spare;

    inline void init() noexcept;

    inline void alloc(ulint hash_size) noexcept;

    inline void clear() noexcept;

    inline void free() noexcept;

    /** Ensure that there is a spare block for a future insert() */
    void prepare_insert() noexcept;

    /** Clean up after erasing an AHI node
    @param erase   node being erased
    @return buffer block to be freed
    @retval nullptr if no buffer block was freed */
    buf_block_t *cleanup_after_erase(ahi_node *erase) noexcept;

    __attribute__((nonnull))
# if defined UNIV_AHI_DEBUG || defined UNIV_DEBUG
    /** Insert or replace an entry into the hash table.
    @param fold  CRC-32C of rec prefix
    @param rec   B-tree leaf page record
    @param block the buffer block that contains rec */
    void insert(uint32_t fold, const rec_t *rec, buf_block_t *block) noexcept;
# else
    /** Insert or replace an entry into the hash table.
    @param fold  CRC-32C of rec prefix
    @param rec   B-tree leaf page record */
    void insert(uint32_t fold, const rec_t *rec) noexcept;
# endif

    /** Delete a pointer to a record if it exists.
    @param fold  CRC-32C of rec prefix
    @param rec   B-tree leaf page record
    @return whether a record existed and was removed */
    inline bool erase(uint32_t fold, const rec_t *rec) noexcept;
  };

  /** Partitions of the adaptive hash index */
  partition *parts;

  /** innodb_adaptive_hash_index_parts */
  ulong n_parts;

  /** Get an adaptive hash index partition */
  partition &get_part(index_id_t id) const noexcept
  { return parts[id % n_parts]; }

  /** Get an adaptive hash index partition */
  partition &get_part(const dict_index_t &index) const noexcept
  {
    return get_part(index.id);
  }

  /** Create and initialize at startup */
  void create() noexcept;

  void alloc(ulint hash_size) noexcept;

  /** Clear when disabling the adaptive hash index */
  inline void clear() noexcept;

  /** Free at shutdown */
  void free() noexcept;
};

/** The adaptive hash index */
extern btr_sea btr_search;

/** Lock all search latches in exclusive mode. */
void btr_search_x_lock_all() noexcept;
/** Unlock all search latches from exclusive mode. */
void btr_search_x_unlock_all() noexcept;
/** Lock all search latches in shared mode. */
void btr_search_s_lock_all() noexcept;
/** Unlock all search latches from shared mode. */
void btr_search_s_unlock_all() noexcept;
# ifdef UNIV_SEARCH_PERF_STAT
/** Number of successful adaptive hash index lookups */
extern ulint	btr_search_n_succ;
/** Number of failed adaptive hash index lookups */
extern ulint	btr_search_n_hash_fail;
# endif /* UNIV_SEARCH_PERF_STAT */
#else /* BTR_CUR_HASH_ADAPT */
# define btr_search_sys_create()
# define btr_search_sys_free()
# define btr_search_drop_page_hash_index(block, garbage_collect)
# define btr_search_s_lock_all(index)
# define btr_search_s_unlock_all(index)
# define btr_search_move_or_delete_hash_entries(new_block, block)
# define btr_search_update_hash_on_insert(cursor)
# define btr_search_update_hash_on_delete(cursor)
# ifdef UNIV_DEBUG
#  define btr_search_check_marked_free_index(block)
# endif /* UNIV_DEBUG */
#endif /* BTR_CUR_HASH_ADAPT */
