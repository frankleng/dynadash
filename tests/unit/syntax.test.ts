import { putTableRow, shallowUpdateTableRow } from "../../src";
import * as update from "../../src/update";
import { getPutItemInput } from "../../src/write";
import * as write from "../../src/write";

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
      "NONE",
    );
  });

  it("should generate a conditional write query", async () => {
    const query = getPutItemInput(
      "table",
      { id: "yo", context: "asdf", expiresAt: 1234567890 },
      {
        conditionExpressionMapList: [{ key: "id", func: "attribute_not_exists" }],
      },
    );

    expect(query).toEqual({
      TableName: "table",
      Item: {
        id: { S: "yo" },
        context: { S: "asdf" },
        expiresAt: { N: "1234567890" },
      },
      conditionExpressionMapList: [{ key: "id", func: "attribute_not_exists" }],
      ExpressionAttributeNames: { "#id": "id" },
      ConditionExpression: "attribute_not_exists(#id)",
    });
  });
});
