module Database.Hitcask.ListKeys where
import Database.Hitcask.Types
import qualified Data.HashMap.Strict as M
import Control.Concurrent.STM

listKeys :: Hitcask -> IO [Key]
listKeys h = fmap M.keys (readTVarIO $ keys h)

