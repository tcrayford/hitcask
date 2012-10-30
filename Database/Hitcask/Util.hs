module Database.Hitcask.Util where

remove :: Eq a => a -> [a] -> [a]
remove x = filter (not . (== x))

untilM :: (Monad m) => m Bool -> m a -> m [a]
untilM f action = go []
  where go xs = do
                x <- f
                if not x
                  then do
                    y <- action
                    go (y:xs)
                  else return $! xs

