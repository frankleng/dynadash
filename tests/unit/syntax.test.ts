import { shallowUpdateTableRow } from "../../src/shallowUpdate";
import * as update from "../../src/update";

describe("Query syntax", () => {
  afterEach(() => {
    // restore the spy created with spyOn
    jest.restoreAllMocks();
  });
  it("should generate a conditional update query", async () => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    const spy = jest.spyOn(update, "updateTableRow").mockImplementation(() => Promise.resolve());

    await shallowUpdateTableRow<{ hash: string; sort: string; id: string; blah: string; yo: string }>(
      "table",
      { hash: "1", sort: "abc" },
      { yo: "John" },
      [
        {
          key: "id",
          func: "attribute_not_exists",
          logicOp: "OR",
        },
        {
          key: "id",
          op: "=",
          value: "123",
        },
      ],
    );

    expect(spy).toBeCalledWith(
      "table",
      { hash: "1", sort: "abc" },
      {
        ConditionExpression: "attribute_not_exists(#id) OR #id = :idXvv",
        ExpressionAttributeNames: { "#id": "id", "#yo": "yo" },
        UpdateExpression: "SET #yo = :yo",
        expressionAttributeValues: { ":idXvv": "123", ":yo": "John" },
      },
    );
  });
});
