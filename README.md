<h1 align="center">
SAIA - the Scalable AI Accelerator - HPC
</h1>
<p align="center">
<a href="https://docs.hpc.gwdg.de/services/saia"><b>Documentation</b></a> | <a href="https://arxiv.org/abs/2407.00110"><b>Paper</b></a>
</p>

This repository contains the HPC-side components of the Scalable AI Accelerator SAIA, which hosts AI services such as <a href="https://chat-ai.academiccloud.de">Chat AI</a>. The implementation of the remaining components of the complete architecture for Chat AI can be found in two other repos:
- Stand-alone web interface: https://github.com/gwdg/chat-ai
- Server components, incl. API gateway and SSH proxy: https://github.com/gwdg/saia-hub 

<p align="center">
<img alt="Chat AI architecture" src="assets/arch-diagram.png" width="50%">
</p>

Together these repos provide the entire underyling mechanism for Chat AI, which can be generalized as a slurm-native HPC web service.

## SAIA HPC

This repository contains the tools and scripts to deploy a consistent and scalable service on a Slurm-based HPC center with the ability to integrate with the web server provided in <a href="https://github.com/gwdg/saia-hub">SAIA Hub</a>.

### SSH-based proxy

In a typical HPC cluster setting, the high-performance compute nodes that are capable of running Large Language Models (LLMs) may not be directly accessible from the internet. In these circumstances, the requests from the web server would have to go through an entry point to the cluster, for example a login node or service node. Furthermore, direct tunneling and port forwarding may be forbidden as a security mechanism, and only certain protocols such as SSH may be allowed.

Therefore, the HPC proxy runs on the cloud server and uses an SSH key to establish a connection to the cluster's entrypoint, i.e. the login/service node. For security reasons, the SSH key hosted on the cloud server is restricted to always run a single script on the login node, namely `cloud_interface.sh` and is never actually given a shell instance. This prevents direct access to the cluster even if the web server is compromised. The restriction to run this script is implemented by configuring the ForceCommand directive in SSH for this specific SSH key; this can be set in the `~/.ssh/authorized_keys` file of an HPC user or functional account without root access, like this:

```bash
command="/path/to/cloud_interface.sh",no-port-forwarding,no-X11-forwarding ssh-rsa <public_key>
```

### Scheduler

The task of the scheduler script `scheduler.py` is to run reliable and scalable HPC services on a Slurm-based HPC cluster. A configuration for the desired services should be set in `config.json`, and once the scheduler is initialized with `python scheduler.py init`, everything else is done automatically.

When an SSH proxy is running and configured to connect to the HPC center, it periodically sends keep-alive prompts to maintain the established SSH connection. Due to the ForceCommand directive, these prompts actually run the `cloud_interface.sh`, which in turn periodically runs the scheduler. The scheduler maintains the state of active backend jobs in the `services/cluster.services` file, and makes sure that there are always sufficient available jobs to handle the incoming requests, scaling up and down the jobs based on demand. It can also log timestamps of user requests which can be used for accounting purposes.

For each service, some configuration parameters must be provided, most notably a Slurm batch (sbatch) script with which the scheduler can submit Slurm jobs in order to host the service on a high-performance compute node, possibly with GPUs. The submission of Slurm jobs is handled automatically by the scheduler, as it preemptively resubmits jobs that are about to expire and randomly assigns a port number for each job, which the service should listen to for incoming requests.

### Services

The implementation of the scheduler and proxy has been abstracted from the service itself, meaning it should be possible to run any REST service within this framework. As for the Chat AI service, we simply use <a href=https://github.com/vllm-project/vllm>vLLM</a>. vLLM is capable of hosting cutting-edge LLMs with state-of-the-art performance on HPC GPU nodes and even provides OpenAI API compatibility out of the box.

## Getting Started

Clone this repository

```bash
git clone https://github.com/gwdg/saia-hpc
```

Create the SSH key on the cloud server, and add a restricted entry via ForceCommand in the `authorized_keys` file in the HPC cluster following this template:

```bash
command="/path/to/cloud_interface.sh" ssh-rsa <public_key>
``` 

Initialize the cluster configuration:
- Replace the parameters in `config.json` with your custom service setup.
- Modify the corresponding service scripts in `sbatch/` accordingly.
- Run `python scheduler.py init` to initialize the scheduler with `config.json`.
    
## Acknowledgements

We thank all colleagues and partners involved in this project.

## Citation

If you use SAIA or Chat AI in your research or services, please cite us as follows:

```
@misc{doosthosseini2024chataiseamlessslurmnative,
      title={Chat AI: A Seamless Slurm-Native Solution for HPC-Based Services}, 
      author={Ali Doosthosseini and Jonathan Decker and Hendrik Nolte and Julian M. Kunkel},
      year={2024},
      eprint={2407.00110},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2407.00110}, 
}
```