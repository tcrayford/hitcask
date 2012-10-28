{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Hitcask.Specs.QuickCheck where
import Database.Hitcask.Types
import Database.Hitcask.SpecHelper
import Database.Hitcask
import qualified Data.HashMap.Strict as M
import Control.Monad
import Test.QuickCheck.Monadic
import Test.QuickCheck
import Data.Maybe(isNothing)
import qualified Data.ByteString as B

instance Arbitrary B.ByteString where
  arbitrary = fmap B.pack arbitrary

instance Arbitrary HitcaskAction where
  arbitrary = do
    k <- arbitrary
    v <- arbitrary
    elements [Put k v, Delete k, Merge]

newtype HitcaskFilePath = HitcaskFilePath FilePath

instance Arbitrary HitcaskFilePath where
  arbitrary = elements $ map (HitcaskFilePath . ("/tmp/hitcask/arbitrarydb" ++) . show) ([0..10] :: [Integer])

data HitcaskAction =
    Put Key Value
  | Delete Key
  | Merge
  deriving(Show, Eq)

data HitcaskPostCondition =
    KeyHasValue Key Value
  | KeyIsEmpty Key
  deriving(Show, Eq)

propCheckPostConditions :: HitcaskFilePath -> [HitcaskAction] -> Property
propCheckPostConditions (HitcaskFilePath fp) actions = monadicIO $ do
  db <- run $ createEmpty fp
  let postConditions = postConditionsFromActions actions
  run $ runActions db actions
  checkPostConditions db postConditions
  run $ closeDB db

type PostConditions = M.HashMap Key HitcaskPostCondition

postConditionsFromActions :: [HitcaskAction] -> PostConditions
postConditionsFromActions = M.fromList . concatMap postcondition

postcondition :: HitcaskAction -> [(Key, HitcaskPostCondition)]
postcondition (Put k v) = [(k, KeyHasValue k v)]
postcondition (Delete k) = [(k, KeyIsEmpty k)]
postcondition Merge = []

runActions :: Hitcask -> [HitcaskAction] -> IO ()
runActions db actions = forM_ actions (runAction db)

runAction :: Hitcask -> HitcaskAction -> IO ()
runAction db (Put k v) = do
  put db k v
  return ()
runAction db (Delete k) = do
  delete db k
  return ()
runAction db Merge = compact db

checkPostConditions :: Hitcask -> PostConditions -> PropertyM IO ()
checkPostConditions db ps = do
  let ks = M.elems ps
  checked <- run $ mapM (checkCondition db) ks
  assert $ and checked

instance Show HitcaskFilePath where
  show (HitcaskFilePath fp) = fp

checkCondition :: Hitcask -> HitcaskPostCondition -> IO Bool
checkCondition db (KeyHasValue k v) = do
  (Just x) <- get db k
  return $! x == v
checkCondition db (KeyIsEmpty k) = do
  n <- get db k
  return $! isNothing n

-- todo
-- add CloseAndReopen
