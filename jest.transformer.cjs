const fs = require('fs');
const path = require('path');
const ts = require('typescript');

const tsconfigPath = path.join(__dirname, 'tsconfig.json');
const tsconfig = ts.readConfigFile(tsconfigPath, (filePath) =>
  fs.readFileSync(filePath, 'utf8'),
);

if (tsconfig.error) {
  throw new Error(
    ts.flattenDiagnosticMessageText(tsconfig.error.messageText, '\n'),
  );
}

const parsedConfig = ts.parseJsonConfigFileContent(
  tsconfig.config,
  ts.sys,
  __dirname,
);

module.exports = {
  process(sourceText, sourcePath) {
    const result = ts.transpileModule(sourceText, {
      compilerOptions: {
        ...parsedConfig.options,
        module: ts.ModuleKind.CommonJS,
        sourceMap: true,
        inlineSources: true,
      },
      fileName: sourcePath,
    });

    return {
      code: result.outputText,
      map: result.sourceMapText ? JSON.parse(result.sourceMapText) : null,
    };
  },
};
