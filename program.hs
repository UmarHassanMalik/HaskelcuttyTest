import qualified Data.Map as Map
import Data.Maybe (maybeToList)
import Criterion.Main -- For benchmarking

-- Core Data Structures
type Stream a = [a]

data Window = Window { start :: Int, end :: Int } deriving (Show, Eq)

data Slice a = Slice { range :: (Int, Int), partial :: a } deriving (Show, Eq)

-- Aggregate Store: Map from slice start to partial aggregate
type AggregateStore b = Map.Map Int b

-- Aggregate functions
data Aggregator a b = Aggregator {
    lift    :: a -> b,       -- Lift a record into a partial aggregate
    combine :: b -> b -> b,  -- Combine two partial aggregates
    lower   :: b -> a        -- Lower a partial aggregate to a final result
}

-- Discretize: Assign indices to each element in the stream
discretize :: Stream a -> [(Int, a)]
discretize stream = zip [0..] stream

-- Main Stream Processor
processStream :: Aggregator a b -> Stream a -> Int -> Int -> Int -> [(Window, a)]
processStream agg stream windowSize slideSize sessionGap =
    go 0 (discretize stream) Map.empty [] []
  where
    go _ [] _ activeSlices results = results  -- Base case
    go idx ((i, x):xs) store activeSlices results =
        let newPartial = lift agg x
            updatedSlices = updateSlices idx newPartial activeSlices
            finalizedWindows = finalizeWindows idx updatedSlices
            (newStore, sessionResults) = processSessions idx x store
        in go (idx + 1) xs newStore updatedSlices (results ++ finalizedWindows ++ sessionResults)

    -- Update slices for sliding windows
    updateSlices idx partial slices =
        Slice (idx, idx + slideSize) partial : slices

    -- Finalize overlapping sliding windows
    finalizeWindows idx slices =
        [ (Window s e, lower agg (foldl1 (combine agg) (map partial slices)))
        | Slice (s, e) _ <- slices, idx >= e
        ]

    -- Process session windows
    processSessions idx x store =
        let newStore = storeSlice idx (lift agg x) store
            threshold = idx - sessionGap
            cleanedStore = cleanupExpiredSlices threshold newStore
            sessionResult = if idx > sessionGap
                            then Just (aggregateSession cleanedStore threshold idx)
                            else Nothing
        in (cleanedStore, maybeToList sessionResult)

    -- Aggregate session results dynamically
    aggregateSession store start end =
        let sessionValues = querySlices start end store
        in (Window start end, lower agg (foldl1 (combine agg) sessionValues))

-- Efficiently store slices with cleanup for expired windows
storeSlice :: Int -> b -> AggregateStore b -> AggregateStore b
storeSlice start partial store = Map.insert start partial store

cleanupExpiredSlices :: Int -> AggregateStore b -> AggregateStore b
cleanupExpiredSlices threshold store = Map.filterWithKey (\k _ -> k >= threshold) store

querySlices :: Int -> Int -> AggregateStore b -> [b]
querySlices start end store = Map.elems $ Map.filterWithKey inRange store
  where
    inRange k _ = k >= start && k <= end

-- Cutty: Aggregate Sharing Implementation
generateCuts :: Slice a -> Int -> [Cut a]
generateCuts (Slice (s, e) partial) cutSize =
    [ Cut (i, min (i + cutSize) e) partial | i <- [s, s + cutSize .. e - 1] ]

combineCuts :: Aggregator a b -> [Cut b] -> b
combineCuts agg cuts = foldl1 (combine agg) (map cutPartial cuts)

data Cut a = Cut { cutRange :: (Int, Int), cutPartial :: a } deriving (Show, Eq)

-- Benchmarking
benchmarkAggregation :: Stream Int -> Int -> Int -> Int -> IO ()
benchmarkAggregation stream windowSize slideSize sessionGap = defaultMain [
    bench "Sliding Window" $ nf (slidingWindowAggregation stream windowSize slideSize) stream,
    bench "Session Window" $ nf (processStream sumAggregator stream windowSize slideSize sessionGap) stream
  ]

-- Example Application: Sliding and Session Window Aggregation
aggregateStream :: Stream Int -> Int -> Int -> Int -> [Int]
aggregateStream stream windowSize slideSize sessionGap =
    let slidingResults = slidingWindowAggregation stream windowSize slideSize
        sessionResults = detectAndAggregateSessions stream sessionGap
    in slidingResults ++ sessionResults

slidingWindowAggregation :: Stream Int -> Int -> Int -> [Int]
slidingWindowAggregation stream windowSize slideSize =
    map aggregateWindow windows
  where
    discretizedStream = discretize stream
    slices = createSlices discretizedStream
    windows = generateWindows slices windowSize

    createSlices :: [(Int, Int)] -> [Slice Int]
    createSlices ds = map (\(i, x) -> Slice (i, i + slideSize) x) ds

    generateWindows :: [Slice Int] -> Int -> [[Slice Int]]
    generateWindows slices wSize =
        [ take wSize (drop i slices) | i <- [0, slideSize .. length slices - wSize] ]

    aggregateWindow :: [Slice Int] -> Int
    aggregateWindow slices = sum (map partial slices)

detectAndAggregateSessions :: Stream Int -> Int -> [Int]
detectAndAggregateSessions stream gap =
    let sessionWindows = detectSessionWindows gap (discretize stream)
    in map (aggregateSessionWindow stream) sessionWindows

aggregateSessionWindow :: Stream Int -> Window -> Int
aggregateSessionWindow stream (Window s e) = sum (take (e - s + 1) (drop s stream))

-- Detect session windows based on gaps
detectSessionWindows :: Int -> [(Int, a)] -> [Window]
detectSessionWindows gap records = go records Nothing []
  where
    go [] _ acc = reverse acc
    go ((idx, _):xs) Nothing acc = go xs (Just (Window idx idx)) acc
    go ((idx, _):xs) (Just w@(Window s e)) acc
        | idx - e > gap = go xs (Just (Window idx idx)) (w : acc) -- Start a new window
        | otherwise     = go xs (Just (Window s idx)) acc         -- Extend the current window

-- Main Function
main :: IO ()
main = do
    let stream = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    let windowSize = 3
    let slideSize = 2
    let sessionGap = 3

    -- Define the aggregator for summing values
    let sumAggregator = Aggregator {
            lift = id,
            combine = (+),
            lower = id
        }

    -- Process the stream with sliding and session windows
    let results = processStream sumAggregator stream windowSize slideSize sessionGap
    mapM_ print results

    -- Run benchmarks
    benchmarkAggregation stream windowSize slideSize sessionGap
