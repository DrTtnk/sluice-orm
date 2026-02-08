import { Schema as S } from "@effect/schema";
import type * as tf from "type-fest";

export const assertSync = <A, I, R>(schema: S.Schema<A, I, R>, u: tf.WritableDeep<NoInfer<A>>) => {
  S.validateSync(schema, { onExcessProperty: "error" })(u);
};
