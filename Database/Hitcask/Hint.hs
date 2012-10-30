module Database.Hitcask.Hint where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import Database.Hitcask.Put
import Database.Hitcask.Restore
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.UTF8 as U
import Data.Serialize.Put
import Data.Serialize.Get
import qualified Data.HashMap.Strict as M
import System.IO(hFlush)

writeHint :: HintFile -> Key -> ValueLocation -> IO ()
writeHint (LogFile h _) key loc = do
  B.hPut h (hint key loc)
  hFlush h

hint :: Key -> ValueLocation -> B.ByteString
hint key (ValueLocation f vs vp ts) = runPut $ do
  putInt32 vs
  putInt32 vp
  putInt32 ts
  putInt32 (length f)
  putByteString $ B.pack f
  putInt32 (B.length key)
  putByteString key

createHintFile :: FilePath -> IO HintFile
createHintFile fp = openLogFile (fp ++ ".hint")

