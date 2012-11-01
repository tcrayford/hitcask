module Database.Hitcask.Hint where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import Database.Hitcask.Put
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Put
import System.IO(hFlush, hClose)
import System.FilePath

writeHint :: HintFile -> Key -> ValueLocation -> IO ()
writeHint (LogFile h _) key loc = do
  B.hPut h (hint key loc)
  hFlush h

hint :: Key -> ValueLocation -> B.ByteString
hint key (ValueLocation f vs vp ts) = runPut $ do
  putInt32 vs
  putInt32 vp
  putInt32 ts
  putWithLength f
  putInt32 (B.length key)
  putByteString key

putWithLength :: String -> Put
putWithLength s = do
  putInt32 (B.length packed)
  putByteString packed
  where packed = B.pack s

createHintFile :: FilePath -> IO HintFile
createHintFile fp = openLogFile (fp ++ ".hint")

hintFileFor :: MergedLog -> IO HintFile
hintFileFor = openLogFile . hintFilePathFor

hintFilePathFor :: MergedLog -> FilePath
hintFilePathFor l = root </> ts ++ ".hitcask.data.hint"
  where root = dropFileName $ path l
        ts = show $ getTimestamp $ path l

closeHint :: HintFile -> IO ()
closeHint = hClose . handle

