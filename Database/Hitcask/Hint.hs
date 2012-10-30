module Database.Hitcask.Hint where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import Database.Hitcask.Put
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Put
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

