type MongoVersions = ["4.2", "4.4", "5.0", "5.1", "6.0", "6.1", "7.0", "7.1", "8.0"];

type AtlasVersions = ["5.0", "5.1", "6.0", "6.1", "7.0", "7.1", "8.0"];

type _TakeAfter<T extends string[], U extends string> =
  T extends [infer First extends string, ...infer Rest extends string[]] ?
    First extends U ?
      T
    : _TakeAfter<Rest, U>
  : [];

type MinMongo<T extends MongoVersions[number]> =
  _TakeAfter<MongoVersions, T> extends [infer _, ...infer Rest extends string[]] ? Rest[number]
  : never;

type MinAtlas<T extends AtlasVersions[number]> =
  _TakeAfter<AtlasVersions, T> extends [infer _, ...infer Rest extends string[]] ? Rest[number]
  : never;
