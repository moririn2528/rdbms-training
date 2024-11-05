use std::cell::Cell;
use std::rc::Rc;
use std::collections::HashMap;
use std::ops::Index;
use std::ops::IndexMut;

use crate::disk::{PageId, PAGE_SIZE, DiskManager};

#[derive(Debug,thiserror::Error)]
pub enum Error{
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("no free buffer available in the pool")]
    NoFreeBuffer,
}

pub type Page = [u8; PAGE_SIZE];


#[derive(Debug, Default, Clone, Copy, Eq, Hash, PartialEq)]
pub struct BufferId(usize);

pub struct Buffer{
    pub page_id: PageId,
    pub page: Page,
    pub is_dirty: Cell<bool>,
}

pub struct Frame{
    usage_count: u64,
    buffer: Rc<Buffer>,
}

pub struct BufferPool{
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}
impl BufferPool{
    fn size(& self) -> usize{
        self.buffers.len()
    }

    fn evict(&mut self) -> Option<BufferId> {
        // Clock-sweep アルゴリズムで次に削除するバッファを決める
        let pool_size = self.size();
        let mut consecutive_pinned=0;
        let victim_id = loop{
            let next_victim_id = self.next_victim_id;
            let frame = &mut self.buffers[next_victim_id.0];
            if frame.usage_count == 0{
                break self.next_victim_id;
            }
            if Rc::get_mut(&mut frame.buffer).is_some(){
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else{
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size{
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId{
        BufferId((buffer_id.0 + 1) % self.size())
    }

}
impl Index<BufferId> for BufferPool{
    type Output = Frame;

    fn index(&self, buffer_id: BufferId) -> &Self::Output{
        &self.buffers[buffer_id.0]
    }
}
impl IndexMut<BufferId> for BufferPool{
    fn index_mut(&mut self, buffer_id: BufferId) -> &mut Self::Output{
        &mut self.buffers[buffer_id.0]
    }
}

pub struct BufferPoolManager{
    disk: DiskManager,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,
}
impl BufferPoolManager{
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error>{
        if let Some(&buffer_id) = self.page_table.get(&page_id){
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }

        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();
            if buffer.is_dirty.get(){
                self.disk.write_page_data(evict_page_id, &buffer.page)?;
            }
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);
            self.disk.read_page_data(page_id, &mut buffer.page)?;
            frame.usage_count = 1;
        }
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }
}

#[cfg(test)]
mod test{
    use super::*;
    
    fn create_buffer_pool() -> BufferPool{
        BufferPool{
            buffers: vec![
                Frame{usage_count: 0, buffer: Rc::new(Buffer{page_id: PageId(0), page: [0; PAGE_SIZE], is_dirty: Cell::new(false)})},
                Frame{usage_count: 0, buffer: Rc::new(Buffer{page_id: PageId(1), page: [0; PAGE_SIZE], is_dirty: Cell::new(false)})},
            ],
            next_victim_id: BufferId(0),
        }
    }

    #[test]
    fn test_evict(){
        let mut pool = create_buffer_pool();
        assert_eq!(pool.evict(), Some(BufferId(0)));
        {
            let _ = Rc::clone(&mut pool[BufferId(0)].buffer);
            pool[BufferId(0)].usage_count = 1;
            assert_eq!(pool.evict(), Some(BufferId(1)));
            let _ = Rc::clone(&mut pool[BufferId(1)].buffer);
            pool[BufferId(1)].usage_count = 1;
            assert_eq!(pool.evict(), None);
        }
        let _ = Rc::clone(&mut pool[BufferId(1)].buffer);
        assert_eq!(pool.evict(), Some(BufferId(0)));
    }
}