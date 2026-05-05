# Step 01: Project Scaffold and Proto Setup

**Phase:** 1 (MVP)
**Prerequisites:** None
**Produces:** Compilable solution with generated protobuf C# classes

---

## Objective

Create the .NET 10 solution structure, add NuGet dependencies, copy the required
`.proto` files from Apache Hadoop, and configure `Grpc.Tools` to generate C# classes
at build time.

---

## Tasks

### 1.1 Create the Solution and Projects

```bash
cd dotnet/

# Create solution
dotnet new sln -n Gtlm.Hdfs

# Create main library project
dotnet new classlib -n Gtlm.Hdfs.Client -f net10.0 -o src/Gtlm.Hdfs.Client
dotnet sln Gtlm.Hdfs.sln add src/Gtlm.Hdfs.Client/Gtlm.Hdfs.Client.csproj

# Create test project
dotnet new xunit -n Gtlm.Hdfs.Client.Tests -f net10.0 -o tests/Gtlm.Hdfs.Client.Tests
dotnet sln Gtlm.Hdfs.sln add tests/Gtlm.Hdfs.Client.Tests/Gtlm.Hdfs.Client.Tests.csproj
dotnet add tests/Gtlm.Hdfs.Client.Tests/Gtlm.Hdfs.Client.Tests.csproj reference src/Gtlm.Hdfs.Client/Gtlm.Hdfs.Client.csproj
```

### 1.2 Add NuGet Dependencies

**`src/Gtlm.Hdfs.Client/Gtlm.Hdfs.Client.csproj`:**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>Gtlm.Hdfs.Client</RootNamespace>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Google.Protobuf" Version="3.*" />
    <PackageReference Include="Grpc.Tools" Version="2.*" PrivateAssets="All" />
    <PackageReference Include="System.IO.Hashing" Version="9.*" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="10.*" />
    <PackageReference Include="Microsoft.Extensions.Options" Version="10.*" />
  </ItemGroup>

  <ItemGroup>
    <Protobuf Include="Proto\**\*.proto" GrpcServices="None" ProtoRoot="Proto" />
  </ItemGroup>
</Project>
```

**`tests/Gtlm.Hdfs.Client.Tests/Gtlm.Hdfs.Client.Tests.csproj`:**

```xml
<ItemGroup>
  <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.*" />
  <PackageReference Include="xunit" Version="2.*" />
  <PackageReference Include="xunit.runner.visualstudio" Version="2.*" />
  <PackageReference Include="Moq" Version="4.*" />
</ItemGroup>
```

### 1.3 Copy Proto Files from Apache Hadoop

Download the three required `.proto` files from the Apache Hadoop trunk and place them
in `src/Gtlm.Hdfs.Client/Proto/`:

```bash
mkdir -p src/Gtlm.Hdfs.Client/Proto

# datatransfer.proto -- the core data transfer protocol
curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/datatransfer.proto \
  -o src/Gtlm.Hdfs.Client/Proto/datatransfer.proto

# hdfs.proto -- ExtendedBlockProto, ChecksumTypeProto, DatanodeIDProto, etc.
curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/hdfs.proto \
  -o src/Gtlm.Hdfs.Client/Proto/hdfs.proto

# Security.proto -- TokenProto (from hadoop-common)
curl -sL https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/proto/Security.proto \
  -o src/Gtlm.Hdfs.Client/Proto/Security.proto
```

### 1.4 Patch Proto Files for C# Codegen

The Hadoop `.proto` files use `proto2` syntax and Java-specific options. Apply these
modifications:

**In all three files**, add a C# namespace option after the existing options block:

```protobuf
option csharp_namespace = "Gtlm.Hdfs.Client.Proto";
```

**Fix import paths** so they resolve within the `Proto/` directory:
- `datatransfer.proto` imports `"Security.proto"` and `"hdfs.proto"` -- these should
  work as-is if `ProtoRoot` is set to `Proto` in the csproj.
- `hdfs.proto` may import `"Security.proto"` -- verify and fix if needed.

**Remove or stub unused imports.** `hdfs.proto` may import other files
(`HAServiceProtocol.proto`, `acl.proto`, `xattr.proto`, `ECSchema.proto`, etc.) that we
don't need. Two options:
- **Option A (recommended):** Create minimal stub `.proto` files for each import that
  define only the types referenced by messages we use. This avoids pulling in the
  entire Hadoop proto tree.
- **Option B:** Trim `hdfs.proto` to only include the messages we need
  (`ExtendedBlockProto`, `DatanodeIDProto`, `DatanodeInfoProto`, `ChecksumTypeProto`,
  `BlockProto`, `DatanodeInfosProto`, `LocatedBlockProto`, `LocatedBlocksProto`).
  Remove messages and their imports that reference types from files we don't have.

**Specific messages we need from each file:**

| File | Required Messages / Enums |
|------|--------------------------|
| `datatransfer.proto` | `OpReadBlockProto`, `ClientOperationHeaderProto`, `BaseHeaderProto`, `CachingStrategyProto`, `BlockOpResponseProto`, `ReadOpChecksumInfoProto`, `ChecksumProto`, `PacketHeaderProto`, `ClientReadStatusProto`, `Status`, `DataTransferEncryptorMessageProto` |
| `hdfs.proto` | `ExtendedBlockProto`, `ChecksumTypeProto`, `DatanodeIDProto`, `DatanodeInfoProto`, `BlockProto`, `LocatedBlockProto`, `LocatedBlocksProto` |
| `Security.proto` | `TokenProto` |

### 1.5 Create Directory Structure

```bash
# Create source directories
mkdir -p src/Gtlm.Hdfs.Client/{Protocol,Checksum,Net,BlockReading,Configuration,Models}

# Create test directories
mkdir -p tests/Gtlm.Hdfs.Client.Tests/IntegrationTests
```

### 1.6 Verify Build

```bash
cd dotnet/
dotnet build Gtlm.Hdfs.sln
```

The build must succeed with generated C# classes in `obj/` for all proto messages.
Verify by checking that types like `OpReadBlockProto`, `PacketHeaderProto`, and
`ExtendedBlockProto` are accessible from C# code.

### 1.7 Smoke Test -- Generated Types Compile

Create a throwaway file to verify:

```csharp
// src/Gtlm.Hdfs.Client/Proto/_VerifyGenerated.cs (delete after verification)
using Gtlm.Hdfs.Client.Proto;

namespace Gtlm.Hdfs.Client;

internal static class VerifyProtoGenerated
{
    static void Check()
    {
        var _ = new OpReadBlockProto();
        var __ = new PacketHeaderProto();
        var ___ = new BlockOpResponseProto();
        var ____ = new ExtendedBlockProto();
    }
}
```

---

## Acceptance Criteria

- [ ] `dotnet build` succeeds with zero errors
- [ ] Generated C# classes exist for `OpReadBlockProto`, `PacketHeaderProto`,
      `BlockOpResponseProto`, `ReadOpChecksumInfoProto`, `ChecksumProto`,
      `ClientReadStatusProto`, `ExtendedBlockProto`, `DatanodeIDProto`,
      `ChecksumTypeProto`, `TokenProto`, `Status` enum
- [ ] Test project compiles and references the main library
- [ ] Solution file at `dotnet/Gtlm.Hdfs.sln`
- [ ] Directory structure matches the plan (`Protocol/`, `Checksum/`, `Net/`, etc.)

---

## Notes

- The Hadoop proto files are Apache 2.0 licensed. Include the license header in
  copied files and add an attribution notice in the project.
- Pin the Hadoop commit hash when downloading protos to ensure reproducible builds.
  Record the hash in a comment in the csproj or a `PROTO_VERSION` file.
- `proto2` required fields generate non-nullable properties in C# with `Google.Protobuf`.
  This is the correct behavior for our use case.
