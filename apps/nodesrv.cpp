

namespace node {
extern
int nodecore_main();
}  // namespace node


int main(int argc, char **argv)
{
    return node::nodecore_main();
}