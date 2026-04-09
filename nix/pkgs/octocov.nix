{
  lib,
  buildGoModule,
  fetchFromGitHub,
}:

buildGoModule rec {
  pname = "octocov";
  version = "0.74.4";

  src = fetchFromGitHub {
    owner = "k1LoW";
    repo = "octocov";
    rev = "v${version}";
    hash = "sha256-45fxoYzvWKpdeum5Nk50Cm//HGdPmZzEc8IaVfgOfmo=";
  };

  vendorHash = "sha256-hKdLKzyP2dmLfZ5BQfamqHwVmCTqMvffLV2YLJDEIyI=";

  doCheck = false;

  ldflags = [
    "-s"
    "-w"
  ];

  meta = {
    description = "Octocov is a toolkit for collecting code metrics (code coverage, code to test ratio, test execution time and your own custom metrics";
    homepage = "https://github.com/k1LoW/octocov";
    changelog = "https://github.com/k1LoW/octocov/blob/${src.rev}/CHANGELOG.md";
    license = lib.licenses.mit;
    maintainers = with lib.maintainers; [ ];
    mainProgram = "octocov";
  };
}
