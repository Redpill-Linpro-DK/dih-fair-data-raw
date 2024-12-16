# Dynamic ID Generation

Data objects processed by this system require a unique `id` property. When the `id` is absent, it is necessary to generate a synthetic `id` from other data fields.

## ID Generation Syntax

To define the synthetic `id`, use a combination of fields, separated by `+`, with support for nested objects and arrays.

- Top-level fields are referenced directly by name.
- Nested fields use `.` to traverse objects and `[index]` to specify array elements.
- Concatenate multiple fields using `+`.
- Prefix the field(s) with `guid:` or `hash:` to convert the string into a GUID or HASH value

### BNF for ID Field Specification

```ebnf
<prefix>       ::= "guid:" | "hash:"
<synthetic-id> ::= [<prefix>] <field-list>
<field-list>   ::= <field> | <field> "+" <field-list>
<field>        ::= <name> | <name> "." <field> | <name> "[" <index> "]"
<name>         ::= <string>
<index>        ::= <digit>+
```

## Configuration Storage
Store the synthetic id mapping in the App Configuration service with the key format Data:Raw:IdSubstitute:[TypeName] where [TypeName] is the data object type name.

Example Key-Value for App Configuration:

Key: `Data:Raw:IdSubstitute:Person`
Value: `name.first+name.last+contact.phoneNumbers[0]`

>> This configuration is automatically set if you maintain /Source/Schema/domain-objects.yml in the Common repo.

## Example

**JSON Document:**

```json
{
  "name": {
    "first": "John",
    "last": "Doe"
  },
  "contact": {
    "email": "johndoe@example.com",
    "phoneNumbers": ["123-456-7890"]
  }
}
```

Synthetic id definition for the above JSON:

`name.first+name.last+contact.phoneNumbers[0]`

Resulting synthetic id:

`JohnDoe123-456-7890`

Synthetic id definition for the above JSON:

`hash:name.first+name.last+contact.phoneNumbers[0]`

Resulting synthetic id:

`105xKX43BT4PCNfgAT0Y79`

Synthetic id definition for the above JSON:

`guid:name.first+name.last+contact.phoneNumbers[0]`

Resulting synthetic id:

`b7021cf9-0139-4046-9f17-973a39d0ae0f`

